import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(
    level=logging.INFO,  # 表示する最低レベル（DEBUG, INFO, WARNING...）
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",  # 時間 [レベル] 名前: メッセージ
    datefmt="%Y-%m-%d %H:%M:%S",  # 時間の表示形式
)
logger = logging.getLogger(__name__)


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
        # with を始めた時に呼ばれる
        self.conn = psycopg2.connect(self.uri)
        return self  # これが with ... as db の 'db' になる

    def __exit__(self, exc_type, exc_val, exc_tb):
        # with を抜けた時（正常終了でもエラーでも）に呼ばれる
        if self.conn:
            self.conn.close()
            logger.debug("Connection closed automatically.")

    @staticmethod
    def auto_connect(func):
        """
        インスタンスメソッドとしても、クラスメソッドとしても呼べるようにする
        1. self(db) があればそれを使う。
        2. なければ新規接続して実行し、すぐ閉じる。
        """

        def wrapper(cls_or_self, *args, **kwargs):
            # 第一引数が PostgresUtil のインスタンスかどうか判定
            if isinstance(cls_or_self, PostgresUtil) and cls_or_self.conn:
                return func(cls_or_self, *args, **kwargs)
            else:
                # インスタンスがない＝ショートカット呼び出し
                with PostgresUtil() as db:
                    return func(db, *args, **kwargs)

        return wrapper

    # ------------------------------------------------------------------
    # ユーティリティー
    # ------------------------------------------------------------------
    def _preprocess_params(self, param_doc):
        """辞書の中身をPostgreSQLの型に合わせて一括変換する共通処理"""
        cleaned_values = []
        for k, v in param_doc.items():
            # 1. 辞書型(dict)なら JSON文字列に変換 (jsonb用)
            if isinstance(v, dict):
                cleaned_values.append(json.dumps(v, default=str))

            # 2. リスト(list)なら そのまま渡す (psycopg2がtext[]等に自動変換してくれる)
            # 3. それ以外(str, int, bool, datetime)もそのまま
            else:
                cleaned_values.append(v)
        return tuple(cleaned_values)

    def _create_where_str(self, where_doc):
        where_conditions = []
        # 値がリストの場合、「重なり( && )」をデフォルトとする
        for col, val in where_doc.items():
            if isinstance(val, list):
                where_conditions.append(f"{col} && %s")
            else:
                where_conditions.append(f"{col} = %s")

        where_str = " AND ".join(where_conditions)
        return where_str

    def _create_insert_sql(self, table_name, insert_doc):
        """辞書のキーから、INSERT文を作る処理"""
        if not insert_doc:
            raise ValueError("条件が設定されていません")

        # insert
        insert_columns = list(insert_doc.keys())
        insert_column_str = ", ".join(insert_columns)
        placeholder_str = ", ".join(["%s"] * len(insert_columns))
        return (
            f"INSERT INTO {table_name} ({insert_column_str}) VALUES ({placeholder_str})"
        )

    def _create_select_sql(self, table_name, where_doc, select_columns=None):
        """辞書のキーから、SELECT文を作る処理"""
        if not where_doc:
            raise ValueError("条件が設定されていません")

        # where
        where_str = self._create_where_str(where_doc)

        # select
        select_str = "*"
        if select_columns:
            select_str = ", ".join(select_columns)

        return f"SELECT {select_str} FROM {table_name} WHERE {where_str}"

    def _create_update_sql(self, table_name, where_doc, update_doc):
        """辞書のキーから、UPDATE文を作る処理"""
        if not where_doc or not update_doc:
            raise ValueError("条件が設定されていません")

        # where
        where_str = self._create_where_str(where_doc)

        # update
        update_columns = list(update_doc.keys())
        update_conditions = [f"{col} = %s" for col in update_columns]
        update_str = " , ".join(update_conditions)

        return f"UPDATE {table_name} SET {update_str} WHERE {where_str}"

    def _create_update_jsonb_merge_sql(self, table_name, where_doc, update_doc):
        """辞書のキーから、UPDATE文を作る処理（JSONBマージ（追加・更新）用）"""
        where_conditions = [f"{col} = %s" for col in where_doc.keys()]
        where_str = " AND ".join(where_conditions)

        update_conditions = []
        for col in update_doc.keys():
            # 常にマージ構文を使う
            update_conditions.append(
                f"{col} = COALESCE({col}, '{{}}'::jsonb) || %s::jsonb"
            )
        update_str = " , ".join(update_conditions)

        return f"UPDATE {table_name} SET {update_str} WHERE {where_str}"

    def _create_delete_sql(self, table_name, where_doc):
        """辞書のキーから、DELETE文を作る処理"""
        if not where_doc:
            raise ValueError("条件が設定されていません")

        # where
        where_str = self._create_where_str(where_doc)

        return f"DELETE FROM {table_name} WHERE {where_str}"

    def _create_count_sql(self, table_name, where_doc):
        """辞書のキーから、COUNT文を作る処理"""
        # where
        where_str = self._create_where_str(where_doc)

        return f"SELECT COUNT(*) as count FROM {table_name} WHERE {where_str}"

    def _query(self, is_one, sql, params=None):
        """データの取得（全件取得）"""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            if is_one:
                return cur.fetchone()
            else:
                return cur.fetchall()

    def _select(self, is_one, table_name, where_doc, select_columns=None):
        sql = self._create_select_sql(table_name, where_doc, select_columns)
        values = self._preprocess_params(where_doc)
        return self._query(is_one, sql, values)

    # ------------------------------------------------------------------
    # パブリック ※クラスメソッドとインスタンスメソッド併用
    # ------------------------------------------------------------------
    @classmethod
    @auto_connect
    def query_one(self, sql, params=None):
        return self._query(True, sql, params)

    @classmethod
    @auto_connect
    def query_list(self, sql, params=None):
        return self._query(False, sql, params)

    @classmethod
    @auto_connect
    def execute_cud(self, sql, params=None, mode="Execute"):
        """CUD実行と行数確認の共通内部メソッド"""
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            count = cur.rowcount  # 影響を受けた行数を取得
            self.conn.commit()

            if count > 0:
                logger.debug(f"[{mode}] Success: {count} row(s) affected.")
                return True
            else:
                logger.warning(f"[{mode}] Warning: No rows affected.")
                return False

    @classmethod
    @auto_connect
    def execute_use_values(self, sql, params):
        """CUD実行(values)"""
        try:
            with self.conn.cursor() as cur:
                # execute_values を使用
                execute_values(cur, sql, params)
                self.conn.commit()
                logger.debug(f"[execute_values] Success into {params}")
                return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"[execute_values] Error into {params}: {e}")
            return False

    @classmethod
    @auto_connect
    def insert(self, table_name, insert_doc):
        """1件挿入"""
        sql = self._create_insert_sql(table_name, insert_doc)
        values = self._preprocess_params(insert_doc)

        return self.execute_cud(sql, values, f"Insert into {table_name}")

    @classmethod
    @auto_connect
    def insert_many(self, table_name, insert_docs):
        """複数挿入（高速版）"""
        if not insert_docs:
            return True

        # execute_values 用の SQL は、VALUES 以降を動的に生成させるため
        # 基本の形（INSERT INTO table (cols) VALUES %s）にするのがコツ
        columns = list(insert_docs[0].keys())
        column_str = ", ".join(columns)
        sql = f"INSERT INTO {table_name} ({column_str}) VALUES %s"

        # 全データの値をタプルのリストにする
        params_list = [self._preprocess_params(doc) for doc in insert_docs]
        return self.execute_use_values(sql, params_list)

    @classmethod
    @auto_connect
    def select_one(self, table_name, where_doc, select_columns=None):
        return self._select(True, table_name, where_doc, select_columns)

    @classmethod
    @auto_connect
    def select_list(self, table_name, where_doc, select_columns=None):
        return self._select(False, table_name, where_doc, select_columns)

    @classmethod
    @auto_connect
    def count(self, table_name, where_doc):
        """指定した条件に一致するレコード数を取得"""
        sql = self._create_count_sql(table_name, where_doc)
        values = self._preprocess_params(where_doc)

        # 1件だけ取得
        result = self.query_one(sql, values)
        return result["count"] if result else 0

    @classmethod
    @auto_connect
    def update(self, table_name, where_doc, update_doc):
        sql = self._create_update_sql(table_name, where_doc, update_doc)
        update_values = self._preprocess_params(update_doc)
        where_values = self._preprocess_params(where_doc)

        params = tuple(update_values) + tuple(where_values)
        """更新 + 更新行数チェック"""
        return self.execute_cud(sql, params, "Update")

    @classmethod
    @auto_connect
    def update_jsonb_merge(self, table_name, where_doc, update_doc):
        sql = self._create_update_jsonb_merge_sql(table_name, where_doc, update_doc)
        update_values = self._preprocess_params(update_doc)
        where_values = self._preprocess_params(where_doc)

        params = tuple(update_values) + tuple(where_values)
        """更新 + 更新行数チェック"""
        return self.execute_cud(sql, params, "Update")

    @classmethod
    @auto_connect
    def delete(self, table_name, where_doc):
        """削除 + 更新行数チェック"""
        sql = self._create_delete_sql(table_name, where_doc)
        values = self._preprocess_params(where_doc)

        return self.execute_cud(sql, values, f"Delete {table_name}")

    # ------------------------------------------------------------------
    # パブリック ※インスタンスメソッドのみ
    # ------------------------------------------------------------------
    def select_iter(self, sql, params=None):
        """1件ずつ yield するジェネレータ (MongoDBのCursor的な挙動)"""
        # cur を with で囲むと抜けた時に閉じちゃうので、明示的に管理
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        try:
            cur.execute(sql, params)
            for row in cur:
                yield row
        finally:
            cur.close()

    def execute(self, sql, params=None):
        """コミットせず実行だけ行う（手動コミット用）"""
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount

    def commit(self):
        """変更を確定する"""
        if self.conn:
            self.conn.commit()

    def rollback(self):
        """変更を取り消す（エラー時用）"""
        if self.conn:
            self.conn.rollback()
