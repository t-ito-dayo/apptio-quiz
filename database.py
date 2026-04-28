import psycopg2
import psycopg2.extras
import os
import json


def get_db():
    db_url = os.environ.get('NEON_DATABASE_URL') or os.environ['DATABASE_URL']
    if db_url.startswith('postgres://'):
        db_url = db_url.replace('postgres://', 'postgresql://', 1)
    conn = psycopg2.connect(db_url, sslmode='require')
    return DBWrapper(conn)


class RowProxy:
    def __init__(self, row):
        self._row = dict(row)
        self._keys = list(row.keys())

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._row[self._keys[key]]
        return self._row[key]

    def __iter__(self):
        return iter(self._row.values())


class CursorWrapper:
    def __init__(self, cur):
        self._cur = cur

    def fetchone(self):
        row = self._cur.fetchone()
        return RowProxy(row) if row else None

    def fetchall(self):
        return [RowProxy(row) for row in self._cur.fetchall()]


class DBWrapper:
    def __init__(self, conn):
        self._conn = conn

    def execute(self, query, params=None):
        query = query.replace('?', '%s')
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query, params or ())
        return CursorWrapper(cur)

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()


def init_db():
    conn = get_db()

    conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            xp INTEGER DEFAULT 0,
            level INTEGER DEFAULT 1,
            avatar TEXT DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS answers (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            question_id TEXT NOT NULL,
            is_correct INTEGER NOT NULL,
            answered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS badges (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            badge_key TEXT NOT NULL,
            earned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, badge_key),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS questions (
            id TEXT PRIMARY KEY,
            pj_id INTEGER REFERENCES projects(id) ON DELETE SET NULL,
            category TEXT NOT NULL,
            question TEXT NOT NULL,
            choices JSONB NOT NULL,
            answer INTEGER NOT NULL,
            explanation TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 既存のquestions.pyから初回マイグレーション
    count = conn.execute('SELECT COUNT(*) FROM questions').fetchone()[0]
    if count == 0:
        try:
            from data.questions import QUESTIONS as FILE_QUESTIONS
            for q in FILE_QUESTIONS:
                conn.execute(
                    'INSERT INTO questions (id, category, question, choices, answer, explanation) VALUES (?,?,?,?::jsonb,?,?) ON CONFLICT (id) DO NOTHING',
                    (q['id'], q['category'], q['question'], json.dumps(q['choices'], ensure_ascii=False), q['answer'], q['explanation'])
                )
        except ImportError:
            pass

    members = ['伊藤', '大熊', '涌井', '啓舟', 'まなてぃー', '江川', '木塚', '高橋', 'プロ']
    for name in members:
        conn.execute(
            'INSERT INTO users (name) VALUES (?) ON CONFLICT (name) DO NOTHING',
            (name,)
        )

    conn.commit()
    conn.close()


def _row_to_question(row):
    choices = row['choices']
    if isinstance(choices, str):
        choices = json.loads(choices)
    return {
        'id': row['id'],
        'pj_id': row['pj_id'],
        'category': row['category'],
        'question': row['question'],
        'choices': choices,
        'answer': row['answer'],
        'explanation': row['explanation'],
    }


def get_questions(pj_id=None, category=None):
    conn = get_db()
    conditions = []
    params = []
    if pj_id == 'none':
        conditions.append('pj_id IS NULL')
    elif pj_id:
        conditions.append('pj_id = ?')
        params.append(int(pj_id))
    if category:
        conditions.append('category = ?')
        params.append(category)
    query = 'SELECT * FROM questions'
    if conditions:
        query += ' WHERE ' + ' AND '.join(conditions)
    query += ' ORDER BY created_at, id'
    rows = conn.execute(query, params or None).fetchall()
    conn.close()
    return [_row_to_question(r) for r in rows]


def get_question_by_id(q_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM questions WHERE id=?', (q_id,)).fetchone()
    conn.close()
    return _row_to_question(row) if row else None


def get_projects():
    conn = get_db()
    rows = conn.execute('SELECT * FROM projects ORDER BY name').fetchall()
    conn.close()
    return [{'id': r['id'], 'name': r['name']} for r in rows]


def update_question(q_id, category, question, choices, answer, explanation, pj_id=None):
    conn = get_db()
    conn.execute(
        'UPDATE questions SET category=?, question=?, choices=?::jsonb, answer=?, explanation=?, pj_id=? WHERE id=?',
        (category, question, json.dumps(choices, ensure_ascii=False), int(answer), pj_id or None, q_id)
    )
    conn.commit()
    conn.close()


def delete_question(q_id):
    conn = get_db()
    conn.execute('DELETE FROM answers WHERE question_id=?', (q_id,))
    conn.execute('DELETE FROM questions WHERE id=?', (q_id,))
    conn.commit()
    conn.close()


XP_PER_CORRECT = 15

LEVEL_NAMES = [
    (1, 'Apptio見習い'),
    (2, 'TBM入門者'),
    (3, 'コスト分析者'),
    (4, 'ITタワー設計者'),
    (5, 'TBMエキスパート'),
    (6, 'ApptioマスターPM'),
]

# 後方互換のためLEVELSは残す（固定閾値は使わない）
LEVELS = [(lv, 0, name) for lv, name in LEVEL_NAMES]


def _thresholds(total_q):
    xp_per_lv = max(total_q, 1) * XP_PER_CORRECT
    return [(lv, xp_per_lv * i, name) for i, (lv, name) in enumerate(LEVEL_NAMES)]


def get_level(xp, total_q):
    level, title = 1, LEVEL_NAMES[0][1]
    for lv, threshold, name in _thresholds(total_q):
        if xp >= threshold:
            level, title = lv, name
    return level, title


def xp_for_next_level(xp, total_q):
    for lv, threshold, name in _thresholds(total_q):
        if xp < threshold:
            return threshold
    return None


def get_progress(xp, total_q):
    xp_per_lv = max(total_q, 1) * XP_PER_CORRECT
    next_xp = xp_for_next_level(xp, total_q)
    if next_xp is None:
        return 100
    prev_xp = next_xp - xp_per_lv
    return min(int((xp - prev_xp) / xp_per_lv * 100), 100)


BADGES = {
    'first_correct':   {'label': '初正解',       'icon': '🎯', 'desc': '初めて正解した'},
    'streak_3':        {'label': '3連続正解',     'icon': '🔥', 'desc': '3問連続正解'},
    'streak_5':        {'label': '5連続正解',     'icon': '⚡', 'desc': '5問連続正解'},
    'total_10':        {'label': '10問クリア',    'icon': '🏅', 'desc': '合計10問正解'},
    'total_30':        {'label': '30問クリア',    'icon': '🥈', 'desc': '合計30問正解'},
    'total_50':        {'label': '50問クリア',    'icon': '🥇', 'desc': '合計50問正解'},
    'level_3':         {'label': 'Lv3到達',       'icon': '⭐', 'desc': 'レベル3に到達'},
    'level_5':         {'label': 'Lv5到達',       'icon': '🌟', 'desc': 'レベル5に到達'},
    'perfect_set':     {'label': 'パーフェクト',  'icon': '💎', 'desc': '10問連続正解'},
}

def check_badges(user_id, conn):
    new_badges = []

    correct = conn.execute(
        'SELECT COUNT(*) FROM answers WHERE user_id=? AND is_correct=1', (user_id,)
    ).fetchone()[0]

    user = conn.execute('SELECT xp, level FROM users WHERE id=?', (user_id,)).fetchone()
    level = user['level']

    def award(key):
        conn.execute(
            'INSERT INTO badges (user_id, badge_key) VALUES (?,?) ON CONFLICT (user_id, badge_key) DO NOTHING',
            (user_id, key)
        )
        new_badges.append(key)

    if correct >= 1:
        award('first_correct')
    if correct >= 10:
        award('total_10')
    if correct >= 30:
        award('total_30')
    if correct >= 50:
        award('total_50')
    if level >= 3:
        award('level_3')
    if level >= 5:
        award('level_5')

    recent = conn.execute(
        'SELECT is_correct FROM answers WHERE user_id=? ORDER BY answered_at DESC LIMIT 10',
        (user_id,)
    ).fetchall()
    streak = 0
    for r in recent:
        if r['is_correct']:
            streak += 1
        else:
            break
    if streak >= 3:
        award('streak_3')
    if streak >= 5:
        award('streak_5')
    if streak >= 10:
        award('perfect_set')

    conn.commit()
    return new_badges
