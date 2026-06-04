"""
cleanup_orphan_alarms.py
------------------------
Một lần: xóa các orphan rows trong card_alarm_conditions / card_alarm_states /
quad_alarm_states tham chiếu tới card đã bị xóa.

Nguyên nhân: trước đây delete_<card>_card() không xóa kèm alarm condition/state.
Khi card mới tái sử dụng id (InnoDB reset AUTO_INCREMENT sau restart), nó kế thừa
condition/state cũ → alarm trigger sai ngay lập tức, bỏ qua stable time.

Từ nay delete_<card>_card() đã tự dọn (xem _purge_card_alarm_rows trong db.py),
nên script này chỉ cần chạy MỘT LẦN để dọn dữ liệu rác hiện có.

Chạy:  python cleanup_orphan_alarms.py            # xem trước (dry-run)
       python cleanup_orphan_alarms.py --apply    # thực sự xóa
"""
import sys
import json
import os
import pymysql

CARD_TABLE = {
    'qtag6': 'subdash_qtag6_cards',
    'qtag4': 'subdash_qtag4_cards',
    'qtag3': 'subdash_qtag3_cards',
    'qtag2': 'subdash_qtag2_cards',
    'single3': 'subdash_qtag_single3_cards',
    'pv_only': 'subdash_qtag_pv_cards',
    'pv_dual': 'subdash_qtag_pv_dual_cards',
}


def _load_uri():
    for p in ('config/SMTP_config.json', 'SMTP_config.json'):
        if os.path.exists(p):
            with open(p) as f:
                return json.load(f).get('MYSQL_URI')
    raise SystemExit('Không tìm thấy SMTP_config.json')


def _connect():
    # mysql+pymysql://user:pass@host:port/db
    uri = _load_uri()
    body = uri.split('://', 1)[1]
    creds, hostpart = body.split('@', 1)
    user, password = creds.split(':', 1)
    hostport, db = hostpart.split('/', 1)
    db = db.split('?', 1)[0]
    host, port = (hostport.split(':', 1) + ['3306'])[:2]
    return pymysql.connect(host=host, user=user, password=password,
                           database=db, port=int(port))


def find_orphans(cur):
    cond, state, quad = [], [], []
    cur.execute('SELECT id, card_type, card_id FROM card_alarm_conditions')
    for rid, ct, cid in cur.fetchall():
        t = CARD_TABLE.get(ct)
        if not t:
            cond.append(rid); continue
        cur.execute(f'SELECT 1 FROM {t} WHERE id=%s', (cid,))
        if not cur.fetchone():
            cond.append(rid)
    cur.execute('SELECT id, card_type, card_id FROM card_alarm_states')
    for rid, ct, cid in cur.fetchall():
        t = CARD_TABLE.get(ct)
        if not t:
            state.append(rid); continue
        cur.execute(f'SELECT 1 FROM {t} WHERE id=%s', (cid,))
        if not cur.fetchone():
            state.append(rid)
    cur.execute('SELECT id, quad_id FROM quad_alarm_states')
    for rid, qid in cur.fetchall():
        cur.execute('SELECT 1 FROM subdash_quad_cards WHERE id=%s', (qid,))
        if not cur.fetchone():
            quad.append(rid)
    return cond, state, quad


def main():
    apply = '--apply' in sys.argv
    con = _connect()
    cur = con.cursor()
    cond, state, quad = find_orphans(cur)
    print(f'Orphan card_alarm_conditions: {len(cond)} -> {cond}')
    print(f'Orphan card_alarm_states:     {len(state)} -> {state}')
    print(f'Orphan quad_alarm_states:     {len(quad)} -> {quad}')
    if not apply:
        print('\n[DRY-RUN] Nothing deleted. Re-run with --apply to actually delete.')
        return
    for rid in cond:
        cur.execute('DELETE FROM card_alarm_conditions WHERE id=%s', (rid,))
    for rid in state:
        cur.execute('DELETE FROM card_alarm_states WHERE id=%s', (rid,))
    for rid in quad:
        cur.execute('DELETE FROM quad_alarm_states WHERE id=%s', (rid,))
    con.commit()
    print('\n[OK] Deleted all orphan rows.')


if __name__ == '__main__':
    main()
