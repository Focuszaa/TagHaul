import json
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import logging
logger = logging.getLogger("tagger")

class ScheduleParser:
    @staticmethod
    def calculate_next_run(schedule_type: str, schedule_value: Any, from_time: Optional[datetime] = None) -> Optional[datetime]:
        now = from_time or datetime.now()
        
        if schedule_type == 'once':
            target = datetime.fromisoformat(schedule_value['datetime'])
            return target if target > now else None
            
        elif schedule_type == 'daily':
            target_time = datetime.strptime(schedule_value['time'], '%H:%M').time()
            next_run = datetime.combine(now.date(), target_time)
            if next_run <= now:
                next_run += timedelta(days=1)
            if 'days' in schedule_value and schedule_value['days']:
                days_of_week = set(schedule_value['days'])
                while next_run.weekday() not in days_of_week:
                    next_run += timedelta(days=1)
            return next_run
            
        elif schedule_type == 'weekly':
            target_time = datetime.strptime(schedule_value['time'], '%H:%M').time()
            target_weekday = schedule_value['weekday']
            days_ahead = target_weekday - now.weekday()
            if days_ahead <= 0:
                days_ahead += 7
            next_run = datetime.combine(now.date() + timedelta(days=days_ahead), target_time)
            return next_run
            
        elif schedule_type == 'monthly':
            target_time = datetime.strptime(schedule_value['time'], '%H:%M').time()
            target_day = schedule_value['day']
            if now.day < target_day:
                next_date = now.replace(day=target_day)
            else:
                if now.month == 12:
                    next_date = now.replace(year=now.year + 1, month=1, day=target_day)
                else:
                    next_date = now.replace(month=now.month + 1, day=target_day)
            next_run = datetime.combine(next_date, target_time)
            return next_run
            
        elif schedule_type == 'cron':
            try:
                from croniter import croniter
                cron = croniter(schedule_value, now)
                return cron.get_next(datetime)
            except ImportError:
                logger.error("croniter not installed. Install with: pip install croniter")
                return None
        else:
            return None

class BatchScheduler:
    def __init__(self, db_path: str, task_manager):
        self.db_path = db_path
        self.task_manager = task_manager
        self._stop_event = threading.Event()
        self._scheduler_thread = None
        self._running = False
        
    def start(self):
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            return
        self._running = True
        self._stop_event.clear()
        self._scheduler_thread = threading.Thread(target=self._run_loop, daemon=True)
        self._scheduler_thread.start()
        
    def stop(self):
        self._running = False
        self._stop_event.set()
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)
            
    def _run_loop(self):
        while self._running:
            try:
                conn = sqlite3.connect(self.db_path)
                conn.row_factory = sqlite3.Row
                now = datetime.now().isoformat()
                batches = conn.execute("""
                    SELECT * FROM scheduled_batches 
                    WHERE enabled = 1 
                    AND next_run_at <= ?
                    ORDER BY next_run_at ASC
                """, (now,)).fetchall()
                
                for batch in batches:
                    self._execute_batch(dict(batch))
                conn.close()
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
            self._stop_event.wait(60)
            
    def _execute_batch(self, batch: Dict[str, Any]):
        conn = sqlite3.connect(self.db_path)
        try:
            selected_paths = json.loads(batch['selected_paths'])
            root = getattr(self.task_manager, '_get_default_root', lambda: '/mnt/synology/photos')()
            
            task_snapshot = self.task_manager.create_task(
                selected_paths=selected_paths,
                root=root,
                model=batch['model'],
                dry_run=bool(batch['dry_run']),
                prompt=batch['prompt'],
                temperature=batch['temperature']
            )
            
            history_id = uuid.uuid4().hex
            conn.execute("""
                INSERT INTO batch_history (id, batch_id, task_id, started_at, status)
                VALUES (?, ?, ?, ?, ?)
            """, (history_id, batch['id'], task_snapshot['id'], datetime.now().isoformat(), 'running'))
            
            conn.execute("""
                UPDATE scheduled_batches 
                SET last_run_at = ?
                WHERE id = ?
            """, (datetime.now().isoformat(), batch['id']))
            conn.commit()
            
            schedule_value = json.loads(batch['schedule_value'])
            next_run = ScheduleParser.calculate_next_run(batch['schedule_type'], schedule_value)
            
            if next_run:
                conn.execute("""
                    UPDATE scheduled_batches 
                    SET next_run_at = ?
                    WHERE id = ?
                """, (next_run.isoformat(), batch['id']))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to execute batch {batch['id']}: {e}")
            conn.execute("""
                UPDATE batch_history 
                SET status = 'failed', completed_at = ?
                WHERE batch_id = ? AND status = 'running'
            """, (datetime.now().isoformat(), batch['id']))
            conn.commit()
        finally:
            conn.close()
