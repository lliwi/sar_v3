#!/usr/bin/env python3
"""
Timezone utilities for SAR v3 application
"""
import os
from datetime import datetime, timezone
import zoneinfo

# Configurar timezone según variable de entorno
TZ = os.getenv('TZ', 'Europe/Madrid')
try:
    LOCAL_TIMEZONE = zoneinfo.ZoneInfo(TZ)
except Exception:
    LOCAL_TIMEZONE = None

def get_local_time():
    """Obtener tiempo local según configuración TZ"""
    if LOCAL_TIMEZONE:
        return datetime.now(LOCAL_TIMEZONE)
    else:
        return datetime.now()

def utc_to_local(utc_datetime):
    """Convertir datetime UTC a timezone local"""
    if not utc_datetime:
        return None

    if LOCAL_TIMEZONE:
        # Si el datetime ya tiene timezone info, convertir directamente
        if utc_datetime.tzinfo is not None:
            return utc_datetime.astimezone(LOCAL_TIMEZONE)
        else:
            # Asumir que es UTC sin timezone info y convertir correctamente
            # Primero marcar como UTC, luego convertir
            utc_aware = utc_datetime.replace(tzinfo=timezone.utc)
            return utc_aware.astimezone(LOCAL_TIMEZONE)
    else:
        # Si no hay timezone configurado, devolver como está
        return utc_datetime

def format_local_datetime(utc_datetime, format_str='%d/%m/%Y %H:%M'):
    """Formatear datetime UTC a string en timezone local"""
    if not utc_datetime:
        return None

    local_dt = utc_to_local(utc_datetime)
    return local_dt.strftime(format_str)

def get_timezone_name():
    """Obtener el nombre del timezone configurado"""
    return TZ