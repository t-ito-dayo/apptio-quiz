import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'quiz.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            xp INTEGER DEFAULT 0,
            level INTEGER DEFAULT 1,
            avatar TEXT DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS answers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            question_id TEXT NOT NULL,
            is_correct INTEGER NOT NULL,
            answered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS badges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            badge_key TEXT NOT NULL,
            earned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, badge_key),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    ''')

    conn.commit()

    # メンバー初期登録
    members = ['伊藤', '大熊', '涌井', '啓舟', 'まなてぃー', '江川', '木塚', '高橋', 'プロ']
    for name in members:
        try:
            conn.execute('INSERT INTO users (name) VALUES (?)', (name,))
        except:
            pass
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
    c = conn.cursor()
    new_badges = []

    correct = c.execute(
        'SELECT COUNT(*) FROM answers WHERE user_id=? AND is_correct=1', (user_id,)
    ).fetchone()[0]

    user = c.execute('SELECT xp, level FROM users WHERE id=?', (user_id,)).fetchone()
    level = user['level']

    def award(key):
        try:
            c.execute('INSERT INTO badges (user_id, badge_key) VALUES (?,?)', (user_id, key))
            new_badges.append(key)
        except sqlite3.IntegrityError:
            pass

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

    # 連続正解チェック
    recent = c.execute(
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
