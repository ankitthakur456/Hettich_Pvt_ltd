import datetime
import ast
import sqlite3
import time
import math


class DBHelper:
    def __init__(self):
        self.connection = sqlite3.connect("HIS.db")
        self.cursor = self.connection.cursor()
        self.cursor.execute("CREATE TABLE IF NOT EXISTS sync_data(ts INTEGER, payload STRING, machine_id STRING)")

    # region Syncronization functions
    def add_sync_data(self, payload, machine_id):
        try:
            ts = int(time.time() * 1000)
            new_payload = dict()
            for i in payload.items():
                if math.isnan(i[1]):
                    data = 'nan'
                else:
                    data = i[1]
                new_payload[i[0]] = data
            print(new_payload)
            # ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.cursor.execute('''INSERT INTO sync_data(ts, payload, machine_id) VALUES (?,?,?)''',
                                (ts, str(new_payload), machine_id))
            print('Successful Sync Payload Added to the database')
            self.connection.commit()
        except Exception as e:
            print(f'ERROR {e} Sync Data not added to the database')

    def get_sync_data(self):
        try:
            sync_payload = list()
            self.cursor.execute('''SELECT machine_id FROM sync_data group by machine_id''')
            machine_ids = self.cursor.fetchall()
            print(machine_ids)
            if machine_ids is not None:
                for at in machine_ids:
                    if at is not None:
                        self.cursor.execute('''SELECT ts, payload FROM sync_data where machine_id=? order by ts ASC''',
                                            (at[0],))
                        data = self.cursor.fetchall()
                        # print(data)
                        if len(data):
                            data_payload = [{"ts": int(item[0]),
                                             "values": ast.literal_eval(item[1]),
                                             }
                                            for item in data]
                            # splitting data_payload in list of lists of 100 items those 100 items are objects
                            # containing ts and values and returning that list as an object
                            sync_payload.append({
                                "machine_id": at[0],
                                "payload": [data_payload[i:i + 100] for i in range(0, len(data_payload), 100)]
                            })

                return sync_payload
            return []
        except Exception as e:
            print(f'ERROR {e} No Sync Data available')
            return []

    def clear_sync_data(self, ts, machine_id):
        try:
            # deleting the payload where ts is less than or equal to ts
            self.cursor.execute("""DELETE FROM sync_data WHERE ts<=? and machine_id=?""", (ts, machine_id))
            self.connection.commit()
            print(f"Successful, Cleared Sync payload from the database for {ts}")
            return True
        except Exception as e:
            print(f'Error in clear_sync_data {e} No sync Data to clear')
            return False

    # endregion


