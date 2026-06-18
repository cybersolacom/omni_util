import inspect
import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from dotenv import load_dotenv
import logging
import functools

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class _AutoConnect:
    """
    クラスから呼ばれたかインスタンスから呼ばれたかで動作を切り替えるディスクリプタ。
    - クラス呼び出し (PostgresUtil.insert(...)):  一時接続を張り、終了後に自動コミット
    - インスタンス呼び出し (db.insert(...)):      既存接続をそのまま使い、コミットしない
    """

    def __init__(self, func):
        self.func = func
        functools.update_wrapper(self, func)

    def __get__(self, obj, objtype=None):
        if obj is None or obj.conn is None:
            # クラス呼び出し → 一時接続＆自動コミット
            if inspect.isgeneratorfunction(self.func):

                @functools.wraps(self.func)
                def one_shot_gen(*args, **kwargs):
                    with objtype() as db:
                        yield from self.func(db, *args, **kwargs)

                return one_shot_gen
            else:

                @functools.wraps(self.func)
                def one_shot(*args, **kwargs):
                    with objtype() as db:
                        return self.func(db, *args, **kwargs)

                return one_shot
        else:
            # インスタンス呼び出し → そのまま実行
            return functools.partial(self.func, obj)


def auto_connect(func):
    return _AutoConnect(func)


class PostgresUtil:
    # ------------------------------------------------------------------
    # 手続き
    # ------------------------------------------------------------------
    def __init__(self):
        self.uri = os.getenv("POSTGRES_URI")
        self.conn = None

    def close(self):
        """接続を閉じる"""
        if self.conn:
            self.conn.close()

    def __enter__(self):
        self.conn = psycopg2.connect(self.uri)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type is None:
                self.conn.commit()
            elif exc_type is not None:
                self.conn.rollback()
            self.conn.close()
            logger.debug("Connection closed automatically.")

    # ------------------------------------------------------------------
    # ユーティリティー
    # ------------------------------------------------------------------
    def _preprocess_params(self, param_doc):
        """辞書の中身をPostgreSQLの型に合わせて一括変換する共通処理"""
        cleaned_values = []
        for k, v in param_doc.items():
            if isinstance(v, dict):
                cleaned_values.append(json.dumps(v, default=str))
            else:
                cleaned_values.append(v)
        return tuple(cleaned_values)

    def _create_where_str(self, where_doc):
        where_conditions = []
        for col, val in where_doc.items():
            if isinstance(val, list):
                where_conditions.append(f"{col} && %s")
            else:
                where_conditions.append(f"{col} = %s")
        return " AND ".join(where_conditions)

    def _create_insert_sql(self, table_name, insert_doc):
        if not insert_doc:
            raise ValueError("条件が設定されていません")
        insert_columns = list(insert_doc.keys())
        insert_column_str = ", ".join(insert_columns)
        placeholder_str = ", ".join(["%s"] * len(insert_columns))
        return (
            f"INSERT INTO {table_name} ({insert_column_str}) VALUES ({placeholder_str})"
        )

    def _create_select_sql(self, table_name, where_doc, select_columns=None):
        if not where_doc:
            raise ValueError("条件が設定されていません")
        where_str = self._create_where_str(where_doc)
        select_str = "*" if not select_columns else ", ".join(select_columns)
        return f"SELECT {select_str} FROM {table_name} WHERE {where_str}"

    def _create_update_sql(self, table_name, where_doc, update_doc):
        if not where_doc or not update_doc:
            raise ValueError("条件が設定されていません")
        where_str = self._create_where_str(where_doc)
        update_str = " , ".join(f"{col} = %s" for col in update_doc.keys())
        return f"UPDATE {table_name} SET {update_str} WHERE {where_str}"

    def _create_update_jsonb_merge_sql(self, table_name, where_doc, update_doc):
        where_str = " AND ".join(f"{col} = %s" for col in where_doc.keys())
        update_str = " , ".join(
            f"{col} = COALESCE({col}, '{{}}'::jsonb) || %s::jsonb"
            for col in update_doc.keys()
        )
        return f"UPDATE {table_name} SET {update_str} WHERE {where_str}"

    def _create_delete_sql(self, table_name, where_doc):
        if not where_doc:
            raise ValueError("条件が設定されていません")
        return f"DELETE FROM {table_name} WHERE {self._create_where_str(where_doc)}"

    def _create_count_sql(self, table_name, where_doc):
        return f"SELECT COUNT(*) as count FROM {table_name} WHERE {self._create_where_str(where_doc)}"

    def _query(self, is_one, sql, params=None):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchone() if is_one else cur.fetchall()

    def _select(self, is_one, table_name, where_doc, select_columns=None):
        sql = self._create_select_sql(table_name, where_doc, select_columns)
        values = self._preprocess_params(where_doc)
        return self._query(is_one, sql, values)

    # ------------------------------------------------------------------
    # パブリック
    # ------------------------------------------------------------------
    @auto_connect
    def query_one(self, sql, params=None):
        return self._query(True, sql, params)

    @auto_connect
    def query_list(self, sql, params=None):
        return self._query(False, sql, params)

    @auto_connect
    def execute_cud(self, sql, params=None, mode="Execute"):
        """CUD実行。スタティック呼び出し時は自動コミット、インスタンス時は__exit__またはdb.commit()で制御"""
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            count = cur.rowcount
            if count > 0:
                logger.debug(f"[{mode}] Success: {count} row(s) affected.")
                return True
            else:
                logger.warning(f"[{mode}] Warning: No rows affected.")
                return False

    @auto_connect
    def execute_use_values(self, sql, params):
        """CUD実行(values)。スタティック呼び出し時は自動コミット、インスタンス時は__exit__またはdb.commit()で制御"""
        with self.conn.cursor() as cur:
            execute_values(cur, sql, params)
            logger.debug(f"[execute_values] Success into {params}")
            return True

    @auto_connect
    def insert(self, table_name, insert_doc):
        """1件挿入"""
        sql = self._create_insert_sql(table_name, insert_doc)
        values = self._preprocess_params(insert_doc)
        return self.execute_cud(sql, values, f"Insert into {table_name}")

    @auto_connect
    def insert_many(self, table_name, insert_docs):
        """複数挿入（高速版）"""
        if not insert_docs:
            return True
        columns = list(insert_docs[0].keys())
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
        params_list = [self._preprocess_params(doc) for doc in insert_docs]
        return self.execute_use_values(sql, params_list)

    @auto_connect
    def select_one(self, table_name, where_doc, select_columns=None):
        return self._select(True, table_name, where_doc, select_columns)

    @auto_connect
    def select_list(self, table_name, where_doc, select_columns=None):
        return self._select(False, table_name, where_doc, select_columns)

    @auto_connect
    def count(self, table_name, where_doc):
        """指定した条件に一致するレコード数を取得"""
        sql = self._create_count_sql(table_name, where_doc)
        values = self._preprocess_params(where_doc)
        result = self.query_one(sql, values)
        return result["count"] if result else 0

    @auto_connect
    def update(self, table_name, where_doc, update_doc):
        """更新 + 更新行数チェック"""
        sql = self._create_update_sql(table_name, where_doc, update_doc)
        params = tuple(self._preprocess_params(update_doc)) + tuple(
            self._preprocess_params(where_doc)
        )
        return self.execute_cud(sql, params, "Update")

    @auto_connect
    def update_jsonb_merge(self, table_name, where_doc, update_doc):
        """更新 + 更新行数チェック（JSONBマージ）"""
        sql = self._create_update_jsonb_merge_sql(table_name, where_doc, update_doc)
        params = tuple(self._preprocess_params(update_doc)) + tuple(
            self._preprocess_params(where_doc)
        )
        return self.execute_cud(sql, params, "Update")

    @auto_connect
    def delete(self, table_name, where_doc):
        """削除 + 更新行数チェック"""
        sql = self._create_delete_sql(table_name, where_doc)
        values = self._preprocess_params(where_doc)
        return self.execute_cud(sql, values, f"Delete {table_name}")

    @auto_connect
    def select_iter(self, sql, params=None):
        """1件ずつ yield するジェネレータ (MongoDBのCursor的な挙動)"""
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        try:
            cur.execute(sql, params)
            for row in cur:
                yield row
        finally:
            cur.close()

    # ------------------------------------------------------------------
    # パブリック ※インスタンスメソッドのみ
    # ------------------------------------------------------------------
    def commit(self):
        """変更を確定する"""
        if self.conn:
            self.conn.commit()

    def rollback(self):
        """変更を取り消す（エラー時用）"""
        if self.conn:
            self.conn.rollback()
