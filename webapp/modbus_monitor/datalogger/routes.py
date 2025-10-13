"""
Datalogger API routes
Provides REST API for managing datalogger orchestra and workers
"""
import os
import sys
import threading
import time
from datetime import datetime, timedelta
from flask import request, jsonify, current_app
from . import datalogger_bp

# Add parent directory for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

try:
    from webapp.modbus_monitor.database.db import (
        list_data_loggers, get_data_logger, 
        purge_tag_logs_by_logger, purge_tag_logs_by_timerange,
        get_tag_logs_count, get_logger_statistics,
        safe_datetime_now
    )
except ImportError as e:
    print(f"Import error in datalogger routes: {e}")

# Global reference to orchestra instance (set by main app)
orchestra_instance = None

def set_orchestra_instance(orchestra):
    """Set the global orchestra instance"""
    global orchestra_instance
    orchestra_instance = orchestra

def get_orchestra():
    """Get the current orchestra instance"""
    global orchestra_instance
    return orchestra_instance

@datalogger_bp.route('/status', methods=['GET'])
def get_status():
    """Get current status of datalogger orchestra and all workers"""
    try:
        orchestra = get_orchestra()
        if not orchestra:
            return jsonify({
                'error': 'Orchestra not running',
                'status': 'offline'
            }), 503
        
        # Get status from orchestra
        status = orchestra.api_get_status()
        
        # Add database information
        db_loggers = list_data_loggers()
        status['database_loggers'] = len(db_loggers)
        status['enabled_loggers'] = len([l for l in db_loggers if l.get('enabled', True)])
        
        return jsonify(status)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@datalogger_bp.route('/reload-config', methods=['POST'])
def reload_config():
    """Reload configuration for all workers"""
    try:
        orchestra = get_orchestra()
        if not orchestra:
            return jsonify({'error': 'Orchestra not running'}), 503
        
        result = orchestra.api_reload_config()
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@datalogger_bp.route('/logger/<int:logger_id>/purge', methods=['POST'])
def purge_logger_logs(logger_id):
    """Purge logs for specific logger"""
    try:
        data = request.get_json() or {}
        
        # Parse date parameters
        days_back = data.get('days_back')
        dt_from_str = data.get('dt_from')
        dt_to_str = data.get('dt_to')
        
        dt_from = None
        dt_to = None
        
        if dt_from_str:
            dt_from = datetime.fromisoformat(dt_from_str)
        if dt_to_str:
            dt_to = datetime.fromisoformat(dt_to_str)
        
        orchestra = get_orchestra()
        if orchestra:
            # Send command to worker
            result = orchestra.api_purge_logs(logger_id, days_back, dt_from, dt_to)
        else:
            # Direct database operation if orchestra not running
            if days_back and not dt_from:
                dt_from = safe_datetime_now() - timedelta(days=days_back)
            if not dt_to:
                dt_to = safe_datetime_now()
            
            deleted_count = purge_tag_logs_by_logger(logger_id, dt_from, dt_to)
            result = {
                'status': 'success',
                'message': f'Deleted {deleted_count} log entries for logger {logger_id}',
                'deleted_count': deleted_count
            }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@datalogger_bp.route('/purge-all', methods=['POST'])
def purge_all_logs():
    """Purge logs across all loggers by time range"""
    try:
        data = request.get_json() or {}
        
        # Parse date parameters
        days_back = data.get('days_back')
        dt_from_str = data.get('dt_from')
        dt_to_str = data.get('dt_to')
        
        if not any([days_back, dt_from_str, dt_to_str]):
            return jsonify({'error': 'Must specify days_back, dt_from, or dt_to'}), 400
        
        dt_from = None
        dt_to = None
        
        if dt_from_str:
            dt_from = datetime.fromisoformat(dt_from_str)
        if dt_to_str:
            dt_to = datetime.fromisoformat(dt_to_str)
        
        if days_back and not dt_from:
            dt_from = safe_datetime_now() - timedelta(days=days_back)
        if not dt_to:
            dt_to = safe_datetime_now()
        
        # Direct database operation
        deleted_count = purge_tag_logs_by_timerange(dt_from, dt_to)
        
        return jsonify({
            'status': 'success',
            'message': f'Deleted {deleted_count} log entries from {dt_from} to {dt_to}',
            'deleted_count': deleted_count,
            'dt_from': dt_from.isoformat() if dt_from else None,
            'dt_to': dt_to.isoformat() if dt_to else None
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@datalogger_bp.route('/logger/<int:logger_id>/stats', methods=['GET'])
def get_logger_stats(logger_id):
    """Get statistics for specific logger"""
    try:
        # Get from database
        stats = get_logger_statistics(logger_id)
        
        # Add runtime stats from orchestra if available
        orchestra = get_orchestra()
        if orchestra:
            orchestra_status = orchestra.api_get_status()
            worker_status = orchestra_status.get('workers', {}).get(logger_id)
            if worker_status:
                stats['runtime'] = worker_status
        
        return jsonify(stats)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@datalogger_bp.route('/loggers', methods=['GET'])
def list_loggers():
    """List all dataloggers with their configuration and status"""
    try:
        # Get database configuration
        db_loggers = list_data_loggers()
        
        # Get runtime status from orchestra
        orchestra = get_orchestra()
        orchestra_status = {}
        if orchestra:
            orchestra_status = orchestra.api_get_status()
        
        # Combine information
        result = []
        for logger in db_loggers:
            logger_id = logger['id']
            
            logger_info = {
                'id': logger_id,
                'name': logger['name'],
                'interval_sec': logger['interval_sec'],
                'enabled': logger['enabled'],
                'description': logger.get('description', ''),
                'created_at': logger.get('created_at'),
                'updated_at': logger.get('updated_at'),
                'tag_count': logger.get('tag_count', 0)
            }
            
            # Add runtime status
            worker_status = orchestra_status.get('workers', {}).get(logger_id)
            if worker_status:
                logger_info['runtime'] = {
                    'running': worker_status['alive'],
                    'pid': worker_status['pid'],
                    'started_at': worker_status['started_at'],
                    'last_seen': worker_status['last_seen'],
                    'restart_count': worker_status['restart_count'],
                    'stats': worker_status.get('stats', {})
                }
            else:
                logger_info['runtime'] = {
                    'running': False,
                    'pid': None
                }
            
            result.append(logger_info)
        
        return jsonify({
            'loggers': result,
            'orchestra_running': orchestra is not None,
            'timestamp': time.time()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@datalogger_bp.route('/logger/<int:logger_id>/restart', methods=['POST'])
def restart_logger(logger_id):
    """Restart specific logger worker"""
    try:
        orchestra = get_orchestra()
        if not orchestra:
            return jsonify({'error': 'Orchestra not running'}), 503
        
        success = orchestra.restart_worker(logger_id)
        
        if success:
            return jsonify({
                'status': 'success',
                'message': f'Logger {logger_id} restart requested'
            })
        else:
            return jsonify({
                'status': 'error',
                'message': f'Failed to restart logger {logger_id}'
            }), 500
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@datalogger_bp.route('/overview', methods=['GET'])
def get_overview():
    """Get overview statistics for all loggers"""
    try:
        # Database stats
        db_loggers = list_data_loggers()
        enabled_count = len([l for l in db_loggers if l.get('enabled', True)])
        
        # Count total log entries
        total_logs = get_tag_logs_count()
        
        # Recent activity (last 24 hours)
        dt_from = safe_datetime_now() - timedelta(hours=24)
        recent_logs = get_tag_logs_count(dt_from=dt_from)
        
        # Orchestra status
        orchestra = get_orchestra()
        orchestra_status = {}
        if orchestra:
            orchestra_status = orchestra.api_get_status()
        
        result = {
            'database': {
                'total_loggers': len(db_loggers),
                'enabled_loggers': enabled_count,
                'total_log_entries': total_logs,
                'recent_entries_24h': recent_logs
            },
            'orchestra': {
                'running': orchestra is not None,
                'active_workers': len(orchestra_status.get('workers', {})) if orchestra else 0,
                'stats': orchestra_status.get('orchestra_stats', {}) if orchestra else {}
            },
            'timestamp': time.time()
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Health check endpoint
@datalogger_bp.route('/health', methods=['GET'])
def health_check():
    """Simple health check"""
    return jsonify({
        'status': 'healthy',
        'timestamp': time.time(),
        'orchestra_running': get_orchestra() is not None
    })