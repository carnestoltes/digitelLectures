import json
import logging
import os
import re
import socket

from contextlib import asynccontextmanager
from decimal import Decimal
from datetime import datetime, timezone
from typing import Any

import pymysql

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool


# ============================================================
# CONFIGURACION
# ============================================================

HVS_HOST = os.getenv("HVS_HOST", "")
HVS_PORT = int(os.getenv("HVS_PORT", ""))

# IMPORTANTE: confirmar el terminador que requiere el equipo.
HVS_COMMAND = os.getenv("HVS_COMMAND", "#HVS-STATUS\r\n").encode("ascii")

TCP_CONNECT_TIMEOUT = 5
TCP_READ_TIMEOUT = 2
MAX_RESPONSE_BYTES = 65536

MYSQL_HOST = os.getenv("MYSQL_HOST", "")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "")

TABLA = "digitel"


def _identificador_sql(nombre: str) -> str:
    """Valida un nombre de tabla/columna que viene de entorno."""
    if not re.fullmatch(r"[A-Za-z0-9_]+", nombre):
        raise ValueError(f"Identificador SQL no válido: {nombre!r}")
    return nombre


# Tabla que relaciona la IP del equipo con su código CNA.
# Ajusta los nombres de columna si en tu tabla son distintos.
CNA_TABLA = _identificador_sql(os.getenv("CNA_TABLA", "digitel_ip_cna"))
CNA_COL_IP = _identificador_sql(os.getenv("CNA_COL_IP", "ip"))
CNA_COL_CODIGO = _identificador_sql(os.getenv("CNA_COL_CODIGO", "cna"))

# Columnas con los valores medidos extraídos del ticket.
COLUMNAS_MEDIDAS = [
    "collecttime_min",
    "presion_media_mbar",
    "temperatura_media_c",
    "presion_ambiente_mbar",
    "temperatura_ambiente_c",
    "cs_20_1013",
    "ca_m100_1006",
    "vm_23_1005_m3",
    "vs_20_1013_m3",
    "va_m100_1006_m3",
    "q_scale_l_min",
]

logger = logging.getLogger("hvs_api")


# ============================================================
# ARRANQUE DE LA APLICACION
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Crea o migra la tabla al arrancar la API."""

    try:
        await run_in_threadpool(inicializar_tabla)
        logger.info("Tabla '%s' lista", TABLA)
    except pymysql.MySQLError as exc:
        # La API arranca igualmente: /ticket y /estado
        # funcionan sin base de datos.
        logger.error("No se pudo inicializar la tabla: %s", exc)

    yield


app = FastAPI(
    title="HVS API",
    description="Consulta TCP/IP, parsing y almacenamiento en MySQL",
    version="1.1.0",
    lifespan=lifespan,
)


# ============================================================
# MODELO DE RESPUESTA
# ============================================================

class StatusResponse(BaseModel):
    id: int
    equipo: str
    codigo_cna: str | None
    fecha_consulta: datetime
    datos_parseados: dict[str, Any]
    ticket_original: str


# ============================================================
# 1. COMUNICACION TCP/IP
# ============================================================

def consultar_equipo() -> str:
    """Envía el comando HVS y devuelve el ticket original."""

    chunks = []
    total_bytes = 0

    with socket.create_connection(
        (HVS_HOST, HVS_PORT),
        timeout=TCP_CONNECT_TIMEOUT,
    ) as sock:

        sock.settimeout(TCP_READ_TIMEOUT)
        sock.sendall(HVS_COMMAND)

        while True:
            try:
                chunk = sock.recv(4096)
            except socket.timeout:
                # El equipo ha dejado de enviar datos temporalmente.
                break

            if not chunk:
                # El equipo ha cerrado la conexión.
                break

            chunks.append(chunk)
            total_bytes += len(chunk)

            if total_bytes > MAX_RESPONSE_BYTES:
                raise ValueError(
                    "El ticket supera el tamaño máximo permitido"
                )

    if not chunks:
        raise TimeoutError(
            "El equipo no ha devuelto datos"
        )

    # Conservamos los saltos de línea del ticket.
    return b"".join(chunks).decode(
        "utf-8",
        errors="replace",
    )


# ============================================================
# 2. PARSER GENERICO
# ============================================================

def parsear_ticket(ticket: str) -> dict[str, Any]:
    """
    Extrae automáticamente las líneas que tengan formato:

        Etiqueta: valor

    No es necesario declarar cada campo previamente.
    Las líneas que no sigan ese formato se conservan
    en el ticket original, pero no se interpretan.
    """

    datos: dict[str, Any] = {}

    for linea in ticket.splitlines():
        linea = linea.strip()

        if not linea or ":" not in linea:
            continue

        etiqueta, valor = linea.split(":", maxsplit=1)

        etiqueta = etiqueta.strip()
        valor = valor.strip()

        if not etiqueta or not valor:
            continue

        # El valor se conserva como texto para no perder
        # unidades, signos, formatos o ceros iniciales.
        #
        # Si una etiqueta aparece varias veces, conservamos
        # todos sus valores en una lista.
        if etiqueta in datos:
            if not isinstance(datos[etiqueta], list):
                datos[etiqueta] = [datos[etiqueta]]

            datos[etiqueta].append(valor)
        else:
            datos[etiqueta] = valor

    return datos


# ============================================================
# 3. CONEXION A MYSQL
# ============================================================

def obtener_conexion_mysql():
    """Abre una conexión nueva a la base de datos."""

    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset="utf8",
        connect_timeout=5,
        read_timeout=10,
        write_timeout=10,
        autocommit=False,
    )


def migrar_tabla(cursor) -> None:
    """
    Adapta una tabla ya existente al esquema actual:
      - añade fecha_ticket si no existe
      - añade columnas de medidas que falten
      - permite NULL en las columnas de medidas
      - crea el índice único (equipo, fecha_ticket)
    Es idempotente: si todo está bien, no hace nada.
    """

    cursor.execute(
        """
        SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """,
        (MYSQL_DATABASE, TABLA),
    )
    columnas = {
        fila[0]: (fila[1], fila[2]) for fila in cursor.fetchall()
    }

    alteraciones = []

    if "fecha_ticket" not in columnas:
        alteraciones.append(
            "ADD COLUMN fecha_ticket DATETIME NULL AFTER fecha_consulta"
        )

    if "codigo_cna" not in columnas:
        alteraciones.append(
            "ADD COLUMN codigo_cna VARCHAR(255) NULL AFTER equipo"
        )

    for columna in COLUMNAS_MEDIDAS:
        if columna not in columnas:
            alteraciones.append(
                f"ADD COLUMN {columna} VARCHAR(255) NULL"
            )
        elif columnas[columna][1] == "NO":
            tipo = columnas[columna][0]
            alteraciones.append(
                f"MODIFY COLUMN {columna} {tipo} NULL"
            )

    cursor.execute(
        """
        SELECT 1
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
          AND INDEX_NAME = 'uq_equipo_fecha_ticket'
        LIMIT 1
        """,
        (MYSQL_DATABASE, TABLA),
    )

    if cursor.fetchone() is None:
        alteraciones.append(
            "ADD UNIQUE INDEX uq_equipo_fecha_ticket "
            "(equipo, fecha_ticket)"
        )

    if alteraciones:
        logger.info(
            "Migrando tabla '%s': %s", TABLA, "; ".join(alteraciones)
        )
        cursor.execute(
            f"ALTER TABLE {TABLA} " + ", ".join(alteraciones)
        )


def inicializar_tabla() -> None:
    """
    Crea la tabla de histórico si no existe y la migra
    si ya existía con un esquema anterior.
    La base de datos debe existir previamente.
    """

    columnas_medidas_sql = ",\n".join(
        f"            {columna} VARCHAR(255) NULL"
        for columna in COLUMNAS_MEDIDAS
    )

    sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLA} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            equipo VARCHAR(255) NOT NULL,
            codigo_cna VARCHAR(255) NULL,
            fecha_consulta DATETIME NOT NULL,
            fecha_ticket DATETIME NULL,
            ticket_original MEDIUMTEXT NOT NULL,
            datos_parseados VARCHAR(1024) NOT NULL,
{columnas_medidas_sql},
            PRIMARY KEY (id),
            UNIQUE INDEX uq_equipo_fecha_ticket (equipo, fecha_ticket),
            INDEX idx_equipo_fecha (equipo, fecha_consulta)
        ) ENGINE=myIsam DEFAULT CHARSET=utf8
    """

    conexion = obtener_conexion_mysql()

    try:
        with conexion.cursor() as cursor:
            cursor.execute(sql)
            migrar_tabla(cursor)
        conexion.commit()
    except Exception:
        conexion.rollback()
        raise
    finally:
        conexion.close()


def numero_decimal(valor):
    """Convierte números como '23,4' o '-99,9' a Decimal."""
    if valor is None:
        return None

    valor = valor.strip().replace(",", ".")

    try:
        return Decimal(valor)
    except Exception:
        return None


def parsear_ticket_estructurado(ticket: str) -> dict:
    # Valores por defecto: NULL en MySQL si el campo no aparece.
    datos: dict[str, Any] = {"fecha_ticket": None}
    datos.update({columna: None for columna in COLUMNAS_MEDIDAS})

    # Fecha y hora que imprime el equipo en el ticket.
    # Ejemplo: "Status:Mo 19.10.26 08:41:55"
    fecha_match = re.search(
        r"\b[A-Za-z]{2}\s+(\d{2}\.\d{2}\.\d{2})"
        r"\s+(\d{2}:\d{2}:\d{2})",
        ticket,
    )

    if fecha_match:
        fecha_texto = (
            f"{fecha_match.group(1)} {fecha_match.group(2)}"
        )

        try:
            datos["fecha_ticket"] = datetime.strptime(
                fecha_texto, "%d.%m.%y %H:%M:%S"
            )
        except ValueError:
            pass

    # Etiquetas antes de los dos puntos.
    patrones = {
        "collecttime_min": r"Collecttime\s*\[min\]\s*:\s*([+-]?\d+(?:[.,]\d+)?)",
        "presion_media_mbar": r"Pr\.\s*avg\.\s*\[mbar\]\s*:\s*([+-]?\d+(?:[.,]\d+)?)",
        "temperatura_media_c": r"Temp\.\s*avg\.\s*\[[^\]]*\]\s*:\s*([+-]?\d+(?:[.,]\d+)?)",
        "presion_ambiente_mbar": r"pr\.av\.amb\.\s*\[mbar\]\s*:\s*([+-]?\d+(?:[.,]\d+)?)",
        "temperatura_ambiente_c": r"Temp\.av\.amb\.\s*\[[^\]]*\]\s*:\s*([+-]?\d+(?:[.,]\d+)?)",
        "cs_20_1013": r"cs\s*\(\s*20\s*/\s*1013\s*\)\s*:\s*([+-]?\d+(?:[.,]\d+)?)",
        "ca_m100_1006": r"ca\s*\(\s*-100\s*/\s*1006\s*\)\s*:\s*([+-]?\d+(?:[.,]\d+)?)",
        # VM es el volumen en condiciones de medida: el equipo imprime
        # "VM( temp/presión)" con la temperatura y presión medias
        # reales (p. ej. "VM( 23/1010)"), que cambian en cada ticket.
        # Se acepta cualquier temperatura (incluso negativa o con
        # decimales) y cualquier presión.
        "vm_23_1005_m3": r"VM\s*\(\s*[+-]?\d+(?:[.,]\d+)?\s*/\s*\d+(?:[.,]\d+)?\s*\)\s*\[\s*m3\s*\]\s*:\s*([+-]?\d+(?:[.,]\d+)?)",
        "vs_20_1013_m3": r"Vs\s*\(\s*20\s*/\s*1013\s*\)\s*\[m3\]\s*:\s*([+-]?\d+(?:[.,]\d+)?)",
        "va_m100_1006_m3": r"Va\s*\(\s*-100\s*/\s*1006\s*\)\s*\[m3\]\s*:\s*([+-]?\d+(?:[.,]\d+)?)",
        "q_scale_l_min": r"at\s+Q\s+Scale\s*:\s*([+-]?\d+(?:[.,]\d+)?)\s*l/min",
    }

    for campo, patron in patrones.items():
        coincidencia = re.search(
            patron, ticket, flags=re.IGNORECASE
        )

        if coincidencia:
            datos[campo] = numero_decimal(
                coincidencia.group(1)
            )

    return datos


# ============================================================
# 4. GUARDAR TICKET Y CAMPOS EXTRAIDOS
# ============================================================

def buscar_codigo_cna(cursor, ip: str) -> str | None:
    """
    Devuelve el código CNA asociado a la IP del equipo en
    la tabla digitel_ip_cna, o None si la IP no está dada de alta.
    """

    sql = (
        f"SELECT {CNA_COL_CODIGO} FROM {CNA_TABLA} "
        f"WHERE {CNA_COL_IP} = %s LIMIT 1"
    )
    cursor.execute(sql, (ip,))
    fila = cursor.fetchone()

    if fila is None or fila[0] is None:
        return None

    return str(fila[0]).strip()


def guardar_ticket_mysql(
    equipo: str,
    ticket: str,
    datos: dict,
    fecha_consulta: datetime,
) -> tuple[int, str | None]:
    """
    Busca el código CNA del equipo e inserta el ticket.
    Si ya existe uno del mismo equipo con la misma fecha_ticket,
    lo actualiza y devuelve su id.
    Si el ticket no trae fecha (fecha_ticket NULL), siempre
    se inserta una fila nueva.
    Devuelve (id, codigo_cna).
    """

    sql = """
        INSERT INTO digitel (
            equipo,
            codigo_cna,
            fecha_consulta,
            fecha_ticket,
            ticket_original,
            datos_parseados,
            collecttime_min,
            presion_media_mbar,
            temperatura_media_c,
            presion_ambiente_mbar,
            temperatura_ambiente_c,
            cs_20_1013,
            ca_m100_1006,
            vm_23_1005_m3,
            vs_20_1013_m3,
            va_m100_1006_m3,
            q_scale_l_min
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            id = LAST_INSERT_ID(id),
            codigo_cna = VALUES(codigo_cna),
            fecha_consulta = VALUES(fecha_consulta),
            ticket_original = VALUES(ticket_original),
            datos_parseados = VALUES(datos_parseados),
            collecttime_min = VALUES(collecttime_min),
            presion_media_mbar = VALUES(presion_media_mbar),
            temperatura_media_c = VALUES(temperatura_media_c),
            presion_ambiente_mbar = VALUES(presion_ambiente_mbar),
            temperatura_ambiente_c = VALUES(temperatura_ambiente_c),
            cs_20_1013 = VALUES(cs_20_1013),
            ca_m100_1006 = VALUES(ca_m100_1006),
            vm_23_1005_m3 = VALUES(vm_23_1005_m3),
            vs_20_1013_m3 = VALUES(vs_20_1013_m3),
            va_m100_1006_m3 = VALUES(va_m100_1006_m3),
            q_scale_l_min = VALUES(q_scale_l_min)
    """

    # Hora de la consulta en UTC (hora del servidor de la API).
    fecha_consulta_mysql = fecha_consulta.astimezone(
        timezone.utc
    ).replace(tzinfo=None)

    # Hora impresa por el equipo, tal cual (hora local del equipo).
    fecha_ticket = datos["fecha_ticket"]

    conexion = obtener_conexion_mysql()

    try:
        with conexion.cursor() as cursor:
            # Código CNA en el momento de la escritura. Si la IP
            # no está en digitel_ip_cna se guarda NULL.
            codigo_cna = buscar_codigo_cna(cursor, equipo)

            if codigo_cna is None:
                logger.warning(
                    "La IP %s no tiene código en %s", equipo, CNA_TABLA
                )

            valores = (
                equipo,
                codigo_cna,
                fecha_consulta_mysql,
                fecha_ticket,
                ticket,
                json.dumps(datos, ensure_ascii=False, default=str),
                *(datos[columna] for columna in COLUMNAS_MEDIDAS),
            )

            cursor.execute(sql, valores)
            ticket_id = cursor.lastrowid

        conexion.commit()
        return ticket_id, codigo_cna

    except Exception:
        conexion.rollback()
        raise

    finally:
        conexion.close()


# ============================================================
# 5. ENDPOINTS FASTAPI
# ============================================================

@app.get("/estado")
def health():
    return {"api": "ok"}


@app.get("/guardaTicket", response_model=StatusResponse)
async def guardar_ticket():

    # Ejecutamos las operaciones bloqueantes fuera
    # del bucle de eventos de FastAPI.
    try:
        ticket = await run_in_threadpool(
            consultar_equipo
        )

    except (OSError, TimeoutError, ValueError) as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Error de comunicación TCP: {exc}",
        ) from exc

    # Parsear el ticket no requiere acceso a la red.
    datos = parsear_ticket_estructurado(ticket)
    fecha = datetime.now(timezone.utc)

    try:
        ticket_id, codigo_cna = await run_in_threadpool(
            guardar_ticket_mysql,
            HVS_HOST,
            ticket,
            datos,
            fecha,
        )

    except pymysql.MySQLError as exc:
        raise HTTPException(
            status_code=500,
            detail=(
                "El equipo respondió, pero no se pudo "
                f"guardar el ticket en MySQL: {exc}"
            ),
        ) from exc

    return StatusResponse(
        id=ticket_id,
        equipo=HVS_HOST,
        codigo_cna=codigo_cna,
        fecha_consulta=fecha,
        datos_parseados=datos,
        ticket_original=ticket,
    )


@app.get("/ticket")
async def obtener_ticket_sin_guardar():
    try:
        # 1. Consultar el equipo por TCP/IP
        ticket = await run_in_threadpool(consultar_equipo)

    except (OSError, TimeoutError, ValueError) as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Error de comunicación TCP: {exc}",
        ) from exc

    # 2. Parsear el ticket
    datos = parsear_ticket(ticket)

    # 3. Devolver JSON sin guardar nada en MySQL
    return {
        "equipo": HVS_HOST,
        "fecha_consulta": datetime.now(timezone.utc).isoformat(),
        "datos_parseados": datos,
        "ticket_original": ticket,
    }


@app.get("/tickets/{ticket_id}")
def obtener_ticket_guardado(ticket_id: int):

    sql = """
        SELECT
            id,
            equipo,
            codigo_cna,
            fecha_consulta,
            fecha_ticket,
            ticket_original,
            datos_parseados
        FROM digitel
        WHERE id = %s
    """

    conexion = obtener_conexion_mysql()

    try:
        with conexion.cursor(
            pymysql.cursors.DictCursor
        ) as cursor:

            cursor.execute(sql, (ticket_id,))
            registro = cursor.fetchone()

    finally:
        conexion.close()

    if registro is None:
        raise HTTPException(
            status_code=404,
            detail="Ticket no encontrado",
        )

    if isinstance(registro["datos_parseados"], str):
        registro["datos_parseados"] = json.loads(
            registro["datos_parseados"]
        )

    return registro


@app.get("/equipos/{equipo}/tickets")
def buscar_tickets_equipo(
    equipo: str,
    limite: int = Query(default=100, ge=1, le=1000),
):
    sql = """
        SELECT *
        FROM digitel
        WHERE equipo = %s
        ORDER BY fecha_consulta DESC
        LIMIT %s
    """

    conexion = obtener_conexion_mysql()

    try:
        with conexion.cursor(
            pymysql.cursors.DictCursor
        ) as cursor:
            cursor.execute(sql, (equipo, limite))
            registros = cursor.fetchall()

        # PyMySQL devuelve el JSON como texto.
        for registro in registros:
            valor = registro.get("datos_parseados")

            if isinstance(valor, str):
                registro["datos_parseados"] = json.loads(valor)

        return {
            "equipo": equipo,
            "total": len(registros),
            "tickets": registros,
        }

    finally:
        conexion.close()
