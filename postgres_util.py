import inspect
import json
import os
import psycopg2
from psycopg2 import sql
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

    def __enter__(self):
        self.conn = psycopg2.connect(self.uri)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            try:
                if exc_type is None:
                    self.conn.commit()
                else:
                    self.conn.rollback()
            finally:
                self.conn.close()
                self.conn = None
                logger.debug("Connection closed and cleared.")

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
        """psycopg2.sql を使用して安全な WHERE 句を構築"""
        # 無条件は許容しない ※カスタムクエリで実装すること
        if not where_doc:
            raise ValueError(
                "条件が設定されていません。無条件はカスタムクエリを使用してください。"
            )

        where_conditions = []
        for col, val in where_doc.items():
            if isinstance(val, list):
                condition = sql.SQL("{} && %s").format(sql.Identifier(col))
            else:
                condition = sql.SQL("{} = %s").format(sql.Identifier(col))
            where_conditions.append(condition)

        return sql.SQL(" AND ").join(where_conditions)

    def _create_insert_sql(self, table_name, insert_doc):
        if not insert_doc:
            raise ValueError("項目が設定されていません")

        columns = insert_doc.keys()
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        )
        return query

    def _create_select_sql(self, table_name, where_doc, select_columns=None):
        where_str = self._create_where_str(where_doc)

        if not select_columns:
            select_str = sql.SQL("*")
        else:
            select_str = sql.SQL(", ").join(map(sql.Identifier, select_columns))

        return sql.SQL("SELECT {} FROM {} WHERE {}").format(
            select_str, sql.Identifier(table_name), where_str
        )

    def _create_update_sql(self, table_name, where_doc, update_doc):
        where_str = self._create_where_str(where_doc)
        update_str = sql.SQL(", ").join(
            sql.SQL("{} = %s").format(sql.Identifier(col)) for col in update_doc.keys()
        )

        return sql.SQL("UPDATE {} SET {} WHERE {}").format(
            sql.Identifier(table_name), update_str, where_str
        )

    def _create_update_jsonb_merge_sql(self, table_name, where_doc, update_doc):
        where_str = self._create_where_str(where_doc)

        # COALESCE(col, '{}'::jsonb) || %s::jsonb を安全に構築
        update_str = sql.SQL(", ").join(
            sql.SQL("{} = COALESCE({}, '{{}}'::jsonb) || %s::jsonb").format(
                sql.Identifier(col), sql.Identifier(col)
            )
            for col in update_doc.keys()
        )

        return sql.SQL("UPDATE {} SET {} WHERE {}").format(
            sql.Identifier(table_name), update_str, where_str
        )

    def _create_delete_sql(self, table_name, where_doc):
        return sql.SQL("DELETE FROM {} WHERE {}").format(
            sql.Identifier(table_name), self._create_where_str(where_doc)
        )

    def _create_count_sql(self, table_name, where_doc):
        return sql.SQL("SELECT COUNT(*) as count FROM {} WHERE {}").format(
            sql.Identifier(table_name), self._create_where_str(where_doc)
        )

    def _query(self, is_one, sql_query, params=None):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql_query, params)
            return cur.fetchone() if is_one else cur.fetchall()

    def _select(self, is_one, table_name, where_doc, select_columns=None):
        sql_query = self._create_select_sql(table_name, where_doc, select_columns)
        values = self._preprocess_params(where_doc)
        return self._query(is_one, sql_query, values)

    # ------------------------------------------------------------------
    # パブリック
    # ------------------------------------------------------------------
    @auto_connect
    def query_one(self, sql_query, params=None):
        return self._query(True, sql_query, params)

    @auto_connect
    def query_list(self, sql_query, params=None):
        return self._query(False, sql_query, params)

    @auto_connect
    def execute_cud(self, sql_query, params=None, mode="Execute"):
        """CUD実行。スタティック呼び出し時は自動コミット、インスタンス時は__exit__またはdb.commit()で制御"""
        with self.conn.cursor() as cur:
            cur.execute(sql_query, params)
            count = cur.rowcount
            logger.debug(f"[{mode}] {count} row(s) affected.")
            return count

    @auto_connect
    def execute_use_values(self, sql_query, params):
        """CUD実行(values)。スタティック呼び出し時は自動コミット、インスタンス時は__exit__またはdb.commit()で制御"""
        with self.conn.cursor() as cur:
            # psycopg2.sql.Composedオブジェクトをexecute_valuesが受け取れるように文字列化
            if hasattr(sql_query, "as_string"):
                sql_query = sql_query.as_string(self.conn)

            execute_values(cur, sql_query, params)
            logger.debug("[execute_values] %d row(s) sent", len(params))
            return True

    @auto_connect
    def insert(self, table_name, insert_doc):
        """1件挿入"""
        sql_query = self._create_insert_sql(table_name, insert_doc)
        values = self._preprocess_params(insert_doc)
        return self.execute_cud(sql_query, values, f"Insert into {table_name}")

    @auto_connect
    def insert_many(self, table_name, insert_docs):
        if not insert_docs:
            logger.warning("インサート件数が0件です")
            return True
        columns = list(insert_docs[0].keys())
        sql_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
        )
        params_list = []
        for doc in insert_docs:
            missing = [c for c in columns if c not in doc]
            if missing:
                raise ValueError(f"行に不足している列があります: {missing}")
            # 列順にそろえてから既存の前処理に通す
            params_list.append(self._preprocess_params({c: doc[c] for c in columns}))
        return self.execute_use_values(sql_query, params_list)

    @auto_connect
    def select_one(self, table_name, where_doc, select_columns=None):
        return self._select(True, table_name, where_doc, select_columns)

    @auto_connect
    def select_list(self, table_name, where_doc, select_columns=None):
        return self._select(False, table_name, where_doc, select_columns)

    @auto_connect
    def count(self, table_name, where_doc):
        """指定した条件に一致するレコード数を取得"""
        sql_query = self._create_count_sql(table_name, where_doc)
        values = self._preprocess_params(where_doc)
        result = self.query_one(sql_query, values)
        return result["count"] if result else 0

    @auto_connect
    def update(self, table_name, where_doc, update_doc):
        """更新 + 更新行数チェック"""
        sql_query = self._create_update_sql(table_name, where_doc, update_doc)
        params = tuple(self._preprocess_params(update_doc)) + tuple(
            self._preprocess_params(where_doc)
        )
        return self.execute_cud(sql_query, params, "Update")

    @auto_connect
    def update_jsonb_merge(self, table_name, where_doc, update_doc):
        """更新 + 更新行数チェック（JSONBマージ）"""
        sql_query = self._create_update_jsonb_merge_sql(
            table_name, where_doc, update_doc
        )
        params = tuple(self._preprocess_params(update_doc)) + tuple(
            self._preprocess_params(where_doc)
        )
        return self.execute_cud(sql_query, params, "Update")

    @auto_connect
    def delete(self, table_name, where_doc):
        """削除 + 更新行数チェック"""
        sql_query = self._create_delete_sql(table_name, where_doc)
        values = self._preprocess_params(where_doc)
        return self.execute_cud(sql_query, values, f"Delete {table_name}")

    @auto_connect
    def select_iter(self, sql_query, params=None):
        """1件ずつ yield するジェネレータ (MongoDBのCursor的な挙動)"""
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        try:
            cur.execute(sql_query, params)
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
