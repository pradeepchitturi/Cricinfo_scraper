"""
Match Tracker - Database-backed version using existing db_config
Tracks downloaded matches in PostgreSQL to prevent duplicate downloads
Note: Table is created via schema.sql, not here
"""
from typing import Optional, Set, Dict, Any
from psycopg2.extras import RealDictCursor
from configs.db_config import get_connection


class MatchTracker:
    """
    Tracks downloaded cricket matches in PostgreSQL database
    to prevent duplicate downloads
    """

    def __init__(
            self,
            schema: str = 'raw',
            table: str = 'match_download_tracker'
    ):
        """
        Initialize MatchTracker

        Args:
            schema: Schema name (default: 'raw')
            table: Table name (default: 'match_download_tracker')
        """
        self.schema = schema
        self.table = table
        self.full_table_name = f"{schema}.{table}"

        # Verify table exists
        self._verify_table()

        # Optional in-memory cache for performance
        self._cache: Optional[Set[str]] = None

    def _verify_table(self):
        """Verify that the tracker table exists"""
        check_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_name = %s
        );
        """

        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(check_table_sql, (self.schema, self.table))
                exists = cursor.fetchone()[0]

                if not exists:
                    raise Exception(
                        f"Table {self.full_table_name} does not exist. "
                        f"Please run schema.sql first using initialize_database()"
                    )

            print(f"Tracker table verified: {self.full_table_name}")
        except Exception as e:
            print(f"Failed to verify tracker table: {e}")
            raise
        finally:
            conn.close()

    def add(
            self,
            match_id: str,
            metadata_rows: int = 0,
            events_rows: int = 0,
            source_url: Optional[str] = None,
            status: str = 'completed'
    ) -> bool:
        """
        Add a match to the tracker

        Args:
            match_id: The match ID to track
            metadata_rows: Number of metadata rows inserted (optional)
            events_rows: Number of event rows inserted (optional)
            source_url: Source URL of the match (optional)
            status: Download status (default: 'completed')

        Returns:
            True if added successfully, False otherwise
        """
        insert_sql = f"""
        INSERT INTO {self.full_table_name} 
            (match_id, metadata_rows, events_rows, source_url, status)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (match_id) 
        DO UPDATE SET 
            downloaded_at = CURRENT_TIMESTAMP,
            metadata_rows = EXCLUDED.metadata_rows,
            events_rows = EXCLUDED.events_rows,
            source_url = EXCLUDED.source_url,
            status = EXCLUDED.status,
            updated_at = CURRENT_TIMESTAMP
        """

        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    insert_sql,
                    (match_id, metadata_rows, events_rows, source_url, status)
                )
            conn.commit()

            # Update cache if it exists
            if self._cache is not None:
                self._cache.add(str(match_id))

            print(f"Match {match_id} tracked successfully")
            return True

        except Exception as e:
            conn.rollback()
            print(f"Error adding match {match_id} to tracker: {e}")
            return False
        finally:
            conn.close()

    def exists(self, match_id: str) -> bool:
        """
        Check if a match has been downloaded

        Args:
            match_id: The match ID to check

        Returns:
            True if match exists in tracker, False otherwise
        """
        # Check cache first (if enabled)
        if self._cache is not None:
            return str(match_id) in self._cache

        check_sql = f"""
        SELECT EXISTS(
            SELECT 1 
            FROM {self.full_table_name} 
            WHERE match_id = %s
        ) as exists
        """

        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(check_sql, (str(match_id),))
                result = cursor.fetchone()
                return result['exists'] if result else False
        except Exception as e:
            print(f"⚠️  Error checking match {match_id}: {e}")
            return False
        finally:
            conn.close()

    def mark_failed(
            self,
            match_id: str,
            error_message: str,
            source_url: Optional[str] = None
    ) -> bool:
        """
        Mark a match download as failed

        Args:
            match_id: The match ID
            error_message: Error message/details
            source_url: Source URL (optional)

        Returns:
            True if marked successfully, False otherwise
        """
        update_sql = f"""
        INSERT INTO {self.full_table_name} 
            (match_id, status, error_message, source_url)
        VALUES (%s, 'failed', %s, %s)
        ON CONFLICT (match_id) 
        DO UPDATE SET 
            status = 'failed',
            error_message = EXCLUDED.error_message,
            source_url = EXCLUDED.source_url,
            updated_at = CURRENT_TIMESTAMP
        """

        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(update_sql, (str(match_id), error_message, source_url))
            conn.commit()
            print(f"⚠️  Match {match_id} marked as failed")
            return True
        except Exception as e:
            conn.rollback()
            print(f"❌ Error marking match {match_id} as failed: {e}")
            return False
        finally:
            conn.close()

    def get_match_info(self, match_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a tracked match

        Args:
            match_id: The match ID to query

        Returns:
            Dictionary with match info or None if not found
        """
        query_sql = f"""
        SELECT 
            match_id,
            downloaded_at,
            status,
            metadata_rows,
            events_rows,
            source_url,
            error_message
        FROM {self.full_table_name}
        WHERE match_id = %s
        """

        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query_sql, (str(match_id),))
                result = cursor.fetchone()
                return dict(result) if result else None
        except Exception as e:
            print(f"❌ Error getting match info for {match_id}: {e}")
            return None
        finally:
            conn.close()

    def get_all_matches(self) -> Set[str]:
        """
        Get all tracked match IDs

        Returns:
            Set of all match IDs
        """
        query_sql = f"SELECT match_id FROM {self.full_table_name}"
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query_sql)
                return {str(row[0]) for row in cursor.fetchall()}
        except Exception as e:
            print(f"❌ Error getting all matches: {e}")
            return set()
        finally:
            conn.close()

    def load_cache(self):
        """
        Load all match IDs into memory cache for faster lookups
        Useful when checking many matches
        """
        self._cache = self.get_all_matches()
        print(f"📦 Loaded {len(self._cache)} matches into cache")

    def clear_cache(self):
        """Clear the in-memory cache"""
        self._cache = None

    def count(self) -> int:
        """
        Get total number of tracked matches

        Returns:
            Count of tracked matches
        """
        count_sql = f"SELECT COUNT(*) FROM {self.full_table_name}"

        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(count_sql)
                return cursor.fetchone()[0]
        except Exception as e:
            print(f"❌ Error counting matches: {e}")
            return 0
        finally:
            conn.close()

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get download statistics

        Returns:
            Dictionary with statistics
        """
        stats_sql = f"""
        SELECT 
            COUNT(*) as total_matches,
            COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
            COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
            COALESCE(SUM(metadata_rows), 0) as total_metadata_rows,
            COALESCE(SUM(events_rows), 0) as total_events_rows,
            MIN(downloaded_at) as first_download,
            MAX(downloaded_at) as last_download
        FROM {self.full_table_name}
        """

        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(stats_sql)
                result = cursor.fetchone()
                return dict(result) if result else {}
        except Exception as e:
            print(f"❌ Error getting statistics: {e}")
            return {}
        finally:
            conn.close()

    def get_failed_matches(self) -> list:
        """
        Get all matches that failed to download

        Returns:
            List of failed match IDs with error messages
        """
        query_sql = f"""
        SELECT match_id, error_message, source_url, downloaded_at
        FROM {self.full_table_name}
        WHERE status = 'failed'
        ORDER BY downloaded_at DESC
        """

        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query_sql)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            print(f"❌ Error getting failed matches: {e}")
            return []
        finally:
            conn.close()

    def get_completed_matches(self) -> list:
        """
        Get all successfully downloaded matches

        Returns:
            List of completed match IDs with details
        """
        query_sql = f"""
        SELECT 
            match_id, 
            downloaded_at, 
            metadata_rows, 
            events_rows,
            source_url
        FROM {self.full_table_name}
        WHERE status = 'completed'
        ORDER BY downloaded_at DESC
        """

        conn = get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query_sql)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            print(f"❌ Error getting completed matches: {e}")
            return []
        finally:
            conn.close()

    def delete_match(self, match_id: str) -> bool:
        """
        Remove a match from tracker (use with caution!)

        Args:
            match_id: Match ID to remove

        Returns:
            True if deleted, False otherwise
        """
        delete_sql = f"DELETE FROM {self.full_table_name} WHERE match_id = %s"

        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(delete_sql, (str(match_id),))
            conn.commit()

            # Update cache if it exists
            if self._cache is not None:
                self._cache.discard(str(match_id))

            print(f"🗑️  Match {match_id} deleted from tracker")
            return True
        except Exception as e:
            conn.rollback()
            print(f"❌ Error deleting match {match_id}: {e}")
            return False
        finally:
            conn.close()

    def print_statistics(self):
        """Print download statistics to console"""
        stats = self.get_statistics()
        if stats:
            print("\n" + "=" * 60)
            print("DOWNLOAD STATISTICS")
            print("=" * 60)
            print(f"Total Matches:        {stats.get('total_matches', 0)}")
            print(f"Completed:            {stats.get('completed', 0)}")
            print(f"Failed:               {stats.get('failed', 0)}")
            print(f"Total Metadata Rows:  {stats.get('total_metadata_rows', 0):,}")
            print(f"Total Events Rows:    {stats.get('total_events_rows', 0):,}")
            print(f"First Download:       {stats.get('first_download', 'N/A')}")
            print(f"Last Download:        {stats.get('last_download', 'N/A')}")
            print("=" * 60 + "\n")