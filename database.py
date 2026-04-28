import psycopg2
import psycopg2.extras
import os


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

    members = ['伊藤', '大熊', '涌井', '啓舟', 'まなてぃー', '江川', '木塚', '高橋', 'プロ']
    for name in members:
        conn.execute(
            'INSERT INTO users (name) VALUES (?) ON CONFLICT (name) DO NOTHING',
            (name,)
        )

    conn.commit()
    conn.close()


LEVELS = [
    (1,    0,   'Apptio見習い'),
    (2,  100,   'TBM入門者'),
    (3,  250,   'コスト分析者'),
    (4,  500,   'ITタワー設計者'),
    (5,  900,   'TBMエキスパート'),
    (6, 1400,   'ApptioマスターPM'),
]

def get_level(xp):
    level, title = 1, LEVELS[0][2]
    for lv, threshold, name in LEVELS:
        if xp >= threshold:
            level, title = lv, name
    return level, title

def xp_for_next_level(xp):
    for lv, threshold, name in LEVELS:
        if xp < threshold:
            return threshold
    return None


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
        result = conn.execute(
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
