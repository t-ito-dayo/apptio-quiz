from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from database import init_db, get_db, get_level, xp_for_next_level, check_badges, BADGES, LEVELS
from data.questions import QUESTIONS
import random
import os
from google import genai as google_genai
from werkzeug.utils import secure_filename

# .envからAPIキー読み込み
_env_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(_env_path):
    for line in open(_env_path):
        k, _, v = line.strip().partition('=')
        if k and v:
            os.environ.setdefault(k, v)

gemini_client = google_genai.Client(api_key=os.environ.get('GEMINI_API_KEY', ''))

AVATAR_FOLDER = os.path.join(os.path.dirname(__file__), 'static', 'avatars')
ALLOWED_EXT = {'png', 'jpg', 'jpeg', 'gif', 'webp'}

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
    user = conn.execute('SELECT * FROM users WHERE id=?', (session['user_id'],)).fetchone()
    level, title = get_level(user['xp'])
    next_xp = xp_for_next_level(user['xp'])
    badges = conn.execute(
        'SELECT badge_key FROM badges WHERE user_id=?', (session['user_id'],)
    ).fetchall()
    badge_keys = [b['badge_key'] for b in badges]

    progress = 0
    if next_xp:
        prev_xp = next((LEVELS[i-1][1] for i, (lv, th, _) in enumerate(LEVELS) if th == next_xp and i > 0), 0)
        progress = int((user['xp'] - prev_xp) / (next_xp - prev_xp) * 100)
    else:
        progress = 100

    # チーム進捗
    all_users = conn.execute('SELECT id, name, xp, avatar FROM users ORDER BY name').fetchall()
    team = []
    total_q = len(QUESTIONS)
    for u in all_users:
        lv, ttl = get_level(u['xp'])
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

    category = request.args.get('category', session.get('quiz_category', 'all'))
    session['quiz_category'] = category

    conn = get_db()
    answered = conn.execute(
        'SELECT question_id FROM answers WHERE user_id=?', (session['user_id'],)
    ).fetchall()
    answered_ids = {r['question_id'] for r in answered}
    conn.close()

    pool = QUESTIONS if category == 'all' else [q for q in QUESTIONS if q['category'] == category]
    remaining = [q for q in pool if q['id'] not in answered_ids]
    if not remaining:
        return redirect(url_for('completed'))

    q = random.choice(remaining)
    session['current_question'] = q['id']
    return render_template('quiz.html', question=q, total=len(pool),
                           answered=len([q for q in pool if q['id'] in answered_ids]))


@app.route('/answer', methods=['POST'])
def answer():
    if 'user_id' not in session:
        return redirect(url_for('index'))

    q_id = session.get('current_question')
    choice = request.form.get('choice', type=int)
    question = next((q for q in QUESTIONS if q['id'] == q_id), None)
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

    user = conn.execute('SELECT xp FROM users WHERE id=?', (session['user_id'],)).fetchone()
    level, level_title = get_level(user['xp'])
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
    wrong_questions = [q for q in QUESTIONS if q['id'] in wrong_ids]
    return render_template('review.html', questions=wrong_questions)


@app.route('/review/answer', methods=['POST'])
def review_answer():
    if 'user_id' not in session:
        return redirect(url_for('index'))

    q_id = request.form.get('question_id')
    choice = request.form.get('choice', type=int)
    question = next((q for q in QUESTIONS if q['id'] == q_id), None)
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
        user = conn.execute('SELECT xp FROM users WHERE id=?', (session['user_id'],)).fetchone()
        level, _ = get_level(user['xp'])
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
    users = conn.execute('SELECT id, name, xp, level, avatar FROM users ORDER BY name').fetchall()
    team_data = []
    for u in users:
        level, title = get_level(u['xp'])
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
        })
    conn.close()
    return render_template('team.html', team=team_data)


@app.route('/completed')
def completed():
    return render_template('completed.html')


@app.route('/mypage')
def mypage():
    if 'user_id' not in session:
        return redirect(url_for('index'))
    conn = get_db()
    user = conn.execute('SELECT * FROM users WHERE id=?', (session['user_id'],)).fetchone()
    level, title = get_level(user['xp'])
    next_xp = xp_for_next_level(user['xp'])

    progress = 0
    if next_xp:
        prev_xp = next((LEVELS[i-1][1] for i, (lv, th, _) in enumerate(LEVELS) if th == next_xp and i > 0), 0)
        progress = int((user['xp'] - prev_xp) / (next_xp - prev_xp) * 100)
    else:
        progress = 100

    # バッジ
    badges = conn.execute('SELECT badge_key FROM badges WHERE user_id=?', (session['user_id'],)).fetchall()
    badge_keys = [b['badge_key'] for b in badges]

    # 全体stats
    total_answered = conn.execute('SELECT COUNT(*) FROM answers WHERE user_id=?', (session['user_id'],)).fetchone()[0]
    total_correct = conn.execute('SELECT COUNT(*) FROM answers WHERE user_id=? AND is_correct=1', (session['user_id'],)).fetchone()[0]

    # カテゴリ別進捗
    from collections import defaultdict
    answered_rows = conn.execute('SELECT question_id, is_correct FROM answers WHERE user_id=?', (session['user_id'],)).fetchall()
    conn.close()

    answered_map = {r['question_id']: r['is_correct'] for r in answered_rows}
    cat_stats = defaultdict(lambda: {'total': 0, 'answered': 0, 'correct': 0})
    for q in QUESTIONS:
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
        total_q=len(QUESTIONS)
    )


@app.route('/avatar/upload', methods=['POST'])
def avatar_upload():
    return redirect(url_for('home'))


@app.route('/quiz/select')
def quiz_select():
    if 'user_id' not in session:
        return redirect(url_for('index'))
    from collections import Counter
    cat_counts = Counter(q['category'] for q in QUESTIONS)
    categories = sorted(cat_counts.items())
    cat_icons = {
        'TBM基礎':   '📘',
        'TBMモデル': '🏗️',
        'Apptio基礎': '⚡',
        'Apptio実務': '🔧',
        'PMスキル':  '🎯',
    }
    return render_template('select.html',
        categories=categories,
        total=len(QUESTIONS),
        cat_icons=cat_icons
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

    knowledge = '\n'.join([
        f"Q: {q['question']}\nA: {q['choices'][q['answer']]}\n解説: {q['explanation']}"
        for q in QUESTIONS
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


if __name__ == '__main__':
    app.run(debug=True)
