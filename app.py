from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from database import (init_db, get_db, get_level, xp_for_next_level, get_progress,
                      check_badges, BADGES, LEVELS, get_questions, get_question_by_id,
                      get_projects, update_question, delete_question)
import random
import os
from google import genai as google_genai
import cloudinary
import cloudinary.uploader

_env_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(_env_path):
    for line in open(_env_path):
        k, _, v = line.strip().partition('=')
        if k and v:
            os.environ.setdefault(k, v)

gemini_client = google_genai.Client(api_key=os.environ.get('GEMINI_API_KEY', ''))

cloudinary.config(
    cloud_name=os.environ.get('CLOUDINARY_CLOUD_NAME', ''),
    api_key=os.environ.get('CLOUDINARY_API_KEY', ''),
    api_secret=os.environ.get('CLOUDINARY_API_SECRET', ''),
)

app = Flask(__name__)
app.secret_key = 'apptio-quiz-secret-2024'

XP_CORRECT = 15
XP_WRONG = 0

init_db()


@app.route('/')
def index():
    if 'user_id' in session:
        return redirect(url_for('home'))
    conn = get_db()
    users = conn.execute('SELECT name FROM users ORDER BY name').fetchall()
    conn.close()
    return render_template('login.html', users=users)


@app.route('/login', methods=['POST'])
def login():
    name = request.form.get('name', '').strip()
    password = request.form.get('password', '').strip()
    if not name:
        conn = get_db()
        users = conn.execute('SELECT name FROM users ORDER BY name').fetchall()
        conn.close()
        return render_template('login.html', users=users, error='メンバーを選択してください')

    site_password = os.environ.get('SITE_PASSWORD', '')
    if not site_password or password != site_password:
        conn = get_db()
        users = conn.execute('SELECT name FROM users ORDER BY name').fetchall()
        conn.close()
        return render_template('login.html', users=users, error='パスワードが違います')

    conn = get_db()
    user = conn.execute('SELECT * FROM users WHERE name=?', (name,)).fetchone()
    if not user:
        conn.execute('INSERT INTO users (name) VALUES (?)', (name,))
        conn.commit()
        user = conn.execute('SELECT * FROM users WHERE name=?', (name,)).fetchone()
    session['user_id'] = user['id']
    session['user_name'] = user['name']
    conn.close()
    return redirect(url_for('home'))


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))


@app.route('/home')
def home():
    if 'user_id' not in session:
        return redirect(url_for('index'))
    conn = get_db()
    total_q = conn.execute('SELECT COUNT(*) FROM questions').fetchone()[0]
    user = conn.execute('SELECT * FROM users WHERE id=?', (session['user_id'],)).fetchone()
    level, title = get_level(user['xp'], total_q)
    next_xp = xp_for_next_level(user['xp'], total_q)
    progress = get_progress(user['xp'], total_q)
    badges = conn.execute(
        'SELECT badge_key FROM badges WHERE user_id=?', (session['user_id'],)
    ).fetchall()
    badge_keys = [b['badge_key'] for b in badges]

    all_users = conn.execute('SELECT id, name, xp, avatar FROM users ORDER BY name').fetchall()
    team = []
    for u in all_users:
        lv, ttl = get_level(u['xp'], total_q)
        answered_count = conn.execute(
            'SELECT COUNT(DISTINCT question_id) FROM answers WHERE user_id=?', (u['id'],)
        ).fetchone()[0]
        team.append({
            'name': u['name'],
            'xp': u['xp'],
            'level': lv,
            'title': ttl,
            'answered': answered_count,
            'progress': int(answered_count / total_q * 100) if total_q > 0 else 0,
            'avatar': u['avatar'],
        })
    conn.close()

    return render_template('home.html',
        user=user, level=level, title=title,
        next_xp=next_xp, progress=progress,
        badges=badge_keys, BADGES=BADGES,
        team=team, total_q=total_q,
        my_name=session['user_name']
    )


@app.route('/quiz')
def quiz():
    if 'user_id' not in session:
        return redirect(url_for('index'))

    category = request.args.get('category')
    pj = request.args.get('pj')

    if category is not None:
        session['quiz_filter'] = {'type': 'category', 'value': category}
    elif pj is not None:
        session['quiz_filter'] = {'type': 'pj', 'value': pj}
    elif 'quiz_filter' not in session:
        session['quiz_filter'] = {'type': 'category', 'value': 'all'}

    f = session['quiz_filter']
    if f['type'] == 'pj':
        pool = get_questions(pj_id=f['value'])
    elif f['value'] == 'all':
        pool = get_questions()
    else:
        pool = get_questions(category=f['value'])

    conn = get_db()
    answered = conn.execute(
        'SELECT question_id FROM answers WHERE user_id=?', (session['user_id'],)
    ).fetchall()
    conn.close()
    answered_ids = {r['question_id'] for r in answered}

    remaining = [q for q in pool if q['id'] not in answered_ids]
    if not remaining:
        return redirect(url_for('completed'))

    q = random.choice(remaining)
    session['current_question'] = q['id']
    return render_template('quiz.html', question=q, total=len(pool),
                           answered=len([x for x in pool if x['id'] in answered_ids]))


@app.route('/answer', methods=['POST'])
def answer():
    if 'user_id' not in session:
        return redirect(url_for('index'))

    q_id = session.get('current_question')
    choice = request.form.get('choice', type=int)
    question = get_question_by_id(q_id)
    if not question or choice is None:
        return redirect(url_for('quiz'))

    is_correct = (choice == question['answer'])
    xp_earned = XP_CORRECT if is_correct else XP_WRONG

    conn = get_db()
    already = conn.execute(
        'SELECT id FROM answers WHERE user_id=? AND question_id=?',
        (session['user_id'], q_id)
    ).fetchone()

    if not already:
        conn.execute(
            'INSERT INTO answers (user_id, question_id, is_correct) VALUES (?,?,?)',
            (session['user_id'], q_id, 1 if is_correct else 0)
        )
        if xp_earned > 0:
            conn.execute('UPDATE users SET xp = xp + ? WHERE id=?',
                         (xp_earned, session['user_id']))
        conn.commit()

    new_badges = check_badges(session['user_id'], conn)

    total_q = conn.execute('SELECT COUNT(*) FROM questions').fetchone()[0]
    user = conn.execute('SELECT xp FROM users WHERE id=?', (session['user_id'],)).fetchone()
    level, level_title = get_level(user['xp'], total_q)
    conn.execute('UPDATE users SET level=? WHERE id=?', (level, session['user_id']))
    conn.commit()
    conn.close()

    return render_template('result.html',
        question=question,
        selected=choice,
        is_correct=is_correct,
        xp_earned=xp_earned,
        new_badges=new_badges,
        BADGES=BADGES
    )


@app.route('/review')
def review():
    if 'user_id' not in session:
        return redirect(url_for('index'))
    conn = get_db()
    wrong = conn.execute(
        'SELECT question_id FROM answers WHERE user_id=? AND is_correct=0',
        (session['user_id'],)
    ).fetchall()
    conn.close()
    wrong_ids = {r['question_id'] for r in wrong}
    all_q = get_questions()
    wrong_questions = [q for q in all_q if q['id'] in wrong_ids]
    return render_template('review.html', questions=wrong_questions)


@app.route('/review/answer', methods=['POST'])
def review_answer():
    if 'user_id' not in session:
        return redirect(url_for('index'))

    q_id = request.form.get('question_id')
    choice = request.form.get('choice', type=int)
    question = get_question_by_id(q_id)
    if not question or choice is None:
        return redirect(url_for('review'))

    is_correct = (choice == question['answer'])
    conn = get_db()
    if is_correct:
        conn.execute(
            'UPDATE answers SET is_correct=1 WHERE user_id=? AND question_id=?',
            (session['user_id'], q_id)
        )
        conn.execute('UPDATE users SET xp = xp + ? WHERE id=?',
                     (XP_CORRECT, session['user_id']))
        conn.commit()
        check_badges(session['user_id'], conn)
        total_q = conn.execute('SELECT COUNT(*) FROM questions').fetchone()[0]
        user = conn.execute('SELECT xp FROM users WHERE id=?', (session['user_id'],)).fetchone()
        level, _ = get_level(user['xp'], total_q)
        conn.execute('UPDATE users SET level=? WHERE id=?', (level, session['user_id']))
        conn.commit()
    conn.close()

    return render_template('result.html',
        question=question,
        selected=choice,
        is_correct=is_correct,
        xp_earned=XP_CORRECT if is_correct else 0,
        new_badges=[],
        BADGES=BADGES,
        from_review=True
    )


@app.route('/team')
def team():
    if 'user_id' not in session:
        return redirect(url_for('index'))
    conn = get_db()
    total_q = conn.execute('SELECT COUNT(*) FROM questions').fetchone()[0]
    users = conn.execute('SELECT id, name, xp, level, avatar FROM users ORDER BY name').fetchall()
    team_data = []
    for u in users:
        level, title = get_level(u['xp'], total_q)
        correct = conn.execute(
            'SELECT COUNT(*) FROM answers WHERE user_id=? AND is_correct=1', (u['id'],)
        ).fetchone()[0]
        total = conn.execute(
            'SELECT COUNT(*) FROM answers WHERE user_id=?', (u['id'],)
        ).fetchone()[0]
        badges = conn.execute(
            'SELECT COUNT(*) FROM badges WHERE user_id=?', (u['id'],)
        ).fetchone()[0]
        team_data.append({
            'name': u['name'],
            'xp': u['xp'],
            'level': level,
            'title': title,
            'correct': correct,
            'total': total,
            'badges': badges,
            'avatar': u['avatar'],
            'progress': get_progress(u['xp'], total_q),
        })
    conn.close()
    return render_template('team.html', team=team_data, total_q=total_q)


@app.route('/completed')
def completed():
    return render_template('completed.html')


@app.route('/mypage')
def mypage():
    if 'user_id' not in session:
        return redirect(url_for('index'))
    conn = get_db()
    total_q = conn.execute('SELECT COUNT(*) FROM questions').fetchone()[0]
    user = conn.execute('SELECT * FROM users WHERE id=?', (session['user_id'],)).fetchone()
    level, title = get_level(user['xp'], total_q)
    next_xp = xp_for_next_level(user['xp'], total_q)
    progress = get_progress(user['xp'], total_q)

    badges = conn.execute('SELECT badge_key FROM badges WHERE user_id=?', (session['user_id'],)).fetchall()
    badge_keys = [b['badge_key'] for b in badges]

    total_answered = conn.execute('SELECT COUNT(*) FROM answers WHERE user_id=?', (session['user_id'],)).fetchone()[0]
    total_correct = conn.execute('SELECT COUNT(*) FROM answers WHERE user_id=? AND is_correct=1', (session['user_id'],)).fetchone()[0]

    answered_rows = conn.execute('SELECT question_id, is_correct FROM answers WHERE user_id=?', (session['user_id'],)).fetchall()
    conn.close()

    answered_map = {r['question_id']: r['is_correct'] for r in answered_rows}
    all_q = get_questions()

    from collections import defaultdict
    cat_stats = defaultdict(lambda: {'total': 0, 'answered': 0, 'correct': 0})
    for q in all_q:
        cat = q['category']
        cat_stats[cat]['total'] += 1
        if q['id'] in answered_map:
            cat_stats[cat]['answered'] += 1
            if answered_map[q['id']]:
                cat_stats[cat]['correct'] += 1

    cat_icons = {
        'TBM基礎': '📘', 'TBMモデル': '🏗️',
        'Apptio基礎': '⚡', 'Apptio実務': '🔧', 'PMスキル': '🎯',
    }

    return render_template('mypage.html',
        user=user, level=level, title=title,
        next_xp=next_xp, progress=progress,
        badge_keys=badge_keys, BADGES=BADGES,
        total_answered=total_answered, total_correct=total_correct,
        cat_stats=cat_stats, cat_icons=cat_icons,
        total_q=len(all_q)
    )


@app.route('/avatar/upload', methods=['POST'])
def avatar_upload():
    if 'user_id' not in session:
        return redirect(url_for('index'))
    file = request.files.get('avatar')
    if not file or file.filename == '':
        return redirect(url_for('mypage'))
    result = cloudinary.uploader.upload(
        file,
        folder='apptio-quiz/avatars',
        public_id=f"user_{session['user_id']}",
        overwrite=True,
        resource_type='image',
    )
    avatar_url = result['secure_url']
    conn = get_db()
    conn.execute('UPDATE users SET avatar=? WHERE id=?', (avatar_url, session['user_id']))
    conn.commit()
    conn.close()
    return redirect(url_for('mypage'))


@app.route('/quiz/select')
def quiz_select():
    if 'user_id' not in session:
        return redirect(url_for('index'))
    from collections import Counter
    all_q = get_questions()
    cat_counts = Counter(q['category'] for q in all_q)
    categories = sorted(cat_counts.items())
    cat_icons = {
        'TBM基礎':   '📘',
        'TBMモデル': '🏗️',
        'Apptio基礎': '⚡',
        'Apptio実務': '🔧',
        'PMスキル':  '🎯',
    }
    projects = get_projects()
    pj_counts = Counter(q['pj_id'] for q in all_q if q['pj_id'])
    return render_template('select.html',
        categories=categories,
        total=len(all_q),
        cat_icons=cat_icons,
        projects=projects,
        pj_counts=pj_counts,
    )


@app.route('/chat')
def chat():
    if 'user_id' not in session:
        return redirect(url_for('index'))
    return render_template('chat.html')


@app.route('/chat/api', methods=['POST'])
def chat_api():
    if 'user_id' not in session:
        return jsonify({'error': 'unauthorized'}), 401

    data = request.get_json()
    user_message = data.get('message', '').strip()
    if not user_message:
        return jsonify({'error': 'empty'}), 400

    questions = get_questions()
    knowledge = '\n'.join([
        f"Q: {q['question']}\nA: {q['choices'][q['answer']]}\n解説: {q['explanation']}"
        for q in questions
    ])

    system_prompt = f"""あなたはApptio・TBM（Technology Business Management）の専門家アシスタントです。
社内のApptio導入プロジェクトメンバーの学習を支援しています。

以下のナレッジベースを参考に、わかりやすく丁寧に答えてください。
ナレッジベース外の質問にも、一般的なApptio・TBMの知識で回答してください。

【ナレッジベース】
{knowledge}

回答は簡潔に、でも要点を押さえてください。箇条書きを活用してください。"""

    response = gemini_client.models.generate_content(
        model='gemini-2.0-flash',
        contents=user_message,
        config=google_genai.types.GenerateContentConfig(
            system_instruction=system_prompt
        )
    )
    return jsonify({'reply': response.text})


# ===== 管理画面 =====

def admin_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated


@app.route('/admin')
@admin_required
def admin():
    keyword = request.args.get('q', '').strip()
    conn = get_db()
    if keyword:
        rows = conn.execute(
            "SELECT q.*, p.name as pj_name FROM questions q LEFT JOIN projects p ON q.pj_id = p.id WHERE q.id ILIKE ? OR q.question ILIKE ? ORDER BY q.created_at, q.id",
            (f'%{keyword}%', f'%{keyword}%')
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT q.*, p.name as pj_name FROM questions q LEFT JOIN projects p ON q.pj_id = p.id ORDER BY q.created_at, q.id"
        ).fetchall()
    conn.close()
    questions = [{'id': r['id'], 'category': r['category'], 'question': r['question'], 'pj_name': r['pj_name']} for r in rows]
    return render_template('admin.html', questions=questions, keyword=keyword)


@app.route('/admin/edit/<q_id>', methods=['GET', 'POST'])
@admin_required
def admin_edit(q_id):
    projects = get_projects()
    if request.method == 'POST':
        choices = [
            request.form.get('choice0', ''),
            request.form.get('choice1', ''),
            request.form.get('choice2', ''),
            request.form.get('choice3', ''),
        ]
        update_question(
            q_id=q_id,
            category=request.form.get('category', ''),
            question=request.form.get('question', ''),
            choices=choices,
            answer=request.form.get('answer', 0),
            explanation=request.form.get('explanation', ''),
            pj_id=request.form.get('pj_id') or None,
        )
        return redirect(url_for('admin'))

    q = get_question_by_id(q_id)
    if not q:
        return '問題が見つかりません', 404
    return render_template('admin_edit.html', q=q, projects=projects)


@app.route('/admin/delete/<q_id>', methods=['POST'])
@admin_required
def admin_delete(q_id):
    delete_question(q_id)
    return redirect(url_for('admin'))


@app.route('/admin/add', methods=['GET', 'POST'])
@admin_required
def admin_add():
    projects = get_projects()
    if request.method == 'POST':
        import time
        q_id = f"q_{int(time.time() * 1000)}"
        choices = [
            request.form.get('choice0', ''),
            request.form.get('choice1', ''),
            request.form.get('choice2', ''),
            request.form.get('choice3', ''),
        ]
        import json as _json
        conn = get_db()
        conn.execute(
            'INSERT INTO questions (id, pj_id, category, question, choices, answer, explanation) VALUES (?,?,?,?,?::jsonb,?,?)',
            (q_id,
             request.form.get('pj_id') or None,
             request.form.get('category', ''),
             request.form.get('question', ''),
             _json.dumps(choices, ensure_ascii=False),
             int(request.form.get('answer', 0)),
             request.form.get('explanation', ''))
        )
        conn.commit()
        conn.close()
        return redirect(url_for('admin'))
    return render_template('admin_add.html', projects=projects)


if __name__ == '__main__':
    app.run(debug=True)
