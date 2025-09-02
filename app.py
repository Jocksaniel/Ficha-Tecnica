import os
import secrets
from datetime import datetime, timedelta
import psycopg2
from flask import Flask, request, jsonify, render_template, send_file
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
import json
import csv
import io
import zipfile
import smtplib # Added back smtplib
import random
import string
from werkzeug.security import generate_password_hash
from email.mime.text import MIMEText # Added back MIMEText

app = Flask(__name__)
CORS(app)

# PostgreSQL database connection details
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'FichaTecnica')
DB_USER = 'FichaTecnica'
DB_PASSWORD = 'C11P2025.'
DB_PORT = os.getenv('DB_PORT', '5432')

# --- Email Configuration ---
EMAIL_SENDER = 'Deinyelbertrs@gmail.com'
EMAIL_PASSWORD = 'mddu ddjo sucd vfuf'
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 465

# Function to establish a database connection
def obtener_conexion_db():
    """
    Establishes and returns a PostgreSQL database connection.
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        return conn
    except psycopg2.Error as e:
        app.logger.error(f"Error al conectar a la base de datos: {e}")
        raise

# Function to create tables if they do not exist
def crear_tablas_si_no_existen():
    """
    Creates necessary tables if they do not exist in the database,
    and populates initial data for locations if the tables are empty.
    """
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        # Create 'usuarios' table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id_usuario SERIAL PRIMARY KEY,
                usuario VARCHAR(20) UNIQUE NOT NULL,
                clave VARCHAR(255) NOT NULL,
                privilegio VARCHAR(50) NOT NULL,
                creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actualizado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS ubicaciones_localidad (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(100) NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS ubicaciones_estados (
                id_estado SERIAL PRIMARY KEY,
                nombre VARCHAR(255) UNIQUE NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS ubicaciones_inmueble (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(100) NOT NULL,
                tipo VARCHAR(50) NOT NULL -- Ej: 'casa', 'edificio', 'apartamento'
            );
        """)

        # Create 'ubicaciones_estados' table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ubicaciones_estados (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(100) NOT NULL
            );
        """)

        # Create 'ubicaciones_municipios' table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ubicaciones_municipios (
                id_municipio SERIAL PRIMARY KEY,
                nombre VARCHAR(100) NOT NULL,
                id_ciudad INTEGER REFERENCES ubicaciones_ciudades(id_ciudad)
            );
        """)

        # Create 'ubicaciones_ciudades' table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ubicaciones_parroquias (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(100) NOT NULL,
                id_municipio INTEGER REFERENCES ubicaciones_municipios(id)
            );
        """)

        # Create 'empleados' table (Updated with new personal and organizational fields)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS empleados (
                id_empleado SERIAL PRIMARY KEY,
                cedula VARCHAR(20) UNIQUE NOT NULL,
                nombres VARCHAR(255) NOT NULL,
                apellidos VARCHAR(255) NOT NULL,
                nacionalidad VARCHAR(50),
                lugar_nacimiento VARCHAR(255),
                fecha_nacimiento DATE,
                edad INTEGER,
                estado_civil VARCHAR(50),
                sexo VARCHAR(50),
                mano_dominante VARCHAR(50),
                num_hijos INTEGER,
                hijos_edades JSONB, -- Store as JSONB
                telefono_habitacion VARCHAR(50),
                telefono_personal VARCHAR(50),
                telefonos_emergencia JSONB, -- Store as JSONB
                profesion VARCHAR(255),
                impedimento_medico_fisico TEXT,
                talla_camisa VARCHAR(50),
                talla_pantalon VARCHAR(50),
                talla_calzado VARCHAR(50),
                condicion_habitacion VARCHAR(50),
                correo_electronico VARCHAR(255),
                codigo_verificacion VARCHAR(6),
                codigo_expira_en TIMESTAMP,
                correo_verificado BOOLEAN DEFAULT FALSE,
                gerencia_general VARCHAR(255),
                gerencia_especifica VARCHAR(255),
                cargo VARCHAR(255),
                esta_estudiando_actualmente BOOLEAN DEFAULT FALSE,
                carrera_actual VARCHAR(255),
                ano_actual VARCHAR(50),
                turno_estudio VARCHAR(50),
                formulario_lleno BOOLEAN DEFAULT FALSE,
                creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actualizado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Create 'direcciones' table (Updated to use location IDs instead of names)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS direcciones (
                id_direccion SERIAL PRIMARY KEY,
                id_empleado INTEGER UNIQUE NOT NULL REFERENCES empleados(id_empleado) ON DELETE CASCADE,
                id_estado INTEGER REFERENCES ubicaciones_estados(id_estado),
                id_municipio INTEGER REFERENCES ubicaciones_municipios(id_municipio),
                id_parroquia INTEGER REFERENCES ubicaciones_parroquias(id_parroquia),
                id_ciudad INTEGER REFERENCES ubicaciones_ciudades(id_ciudad),
                localidad VARCHAR(100),
                nombre_localidad VARCHAR(100),
                tipo_inmueble VARCHAR(50),
                numero_casa VARCHAR(20),
                edificio_bloque_torre VARCHAR(50),
                piso VARCHAR(10),
                puerta VARCHAR(10),
                direccion_detallada TEXT,
                zona_postal VARCHAR(20),
                condicion_habitacion VARCHAR(50),
                creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actualizado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create 'educacion' table (Updated to include tipo_estudio)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS educacion (
                id_educacion SERIAL PRIMARY KEY,
                id_empleado INTEGER NOT NULL REFERENCES empleados(id_empleado) ON DELETE CASCADE,
                nombre_carrera VARCHAR(255),
                nivel_educativo VARCHAR(50),
                fecha_graduacion DATE,
                tipo_estudio VARCHAR(50), -- 'pregrado' or 'postgrado'
                creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Create 'experiencia_laboral' table (Updated with more fields)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS experiencia_laboral (
                id_experiencia SERIAL PRIMARY KEY,
                id_empleado INTEGER NOT NULL REFERENCES empleados(id_empleado) ON DELETE CASCADE,
                nombre_empresa VARCHAR(255),
                fecha_ingreso DATE,
                fecha_egreso DATE,
                dependencia_organizativa VARCHAR(255),
                cargo VARCHAR(255),
                telefono VARCHAR(50),
                ultimo_sueldo NUMERIC(15, 2),
                descripcion TEXT,
                creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Create 'cursos' table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cursos (
                id_curso SERIAL PRIMARY KEY,
                id_empleado INTEGER NOT NULL REFERENCES empleados(id_empleado) ON DELETE CASCADE,
                nombre_curso VARCHAR(255),
                institucion VARCHAR(255),
                fecha_inicio DATE,
                fecha_fin DATE,
                duracion_horas INTEGER,
                creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # NEW TABLE: 'idiomas'
        cur.execute("""
            CREATE TABLE IF NOT EXISTS idiomas (
                id_idioma SERIAL PRIMARY KEY,
                id_empleado INTEGER NOT NULL REFERENCES empleados(id_empleado) ON DELETE CASCADE,
                idioma VARCHAR(100) NOT NULL,
                nivel VARCHAR(50) NOT NULL,
                creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(id_empleado, idioma)
            );
        """)

        # NEW TABLE: 'habilidades'
        cur.execute("""
            CREATE TABLE IF NOT EXISTS habilidades (
                id_habilidad SERIAL PRIMARY KEY,
                id_empleado INTEGER NOT NULL REFERENCES empleados(id_empleado) ON DELETE CASCADE,
                habilidad VARCHAR(255) NOT NULL,
                creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(id_empleado, habilidad)
            );
        """)

        # Create 'realizados' table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS realizados (
                id_realizado SERIAL PRIMARY KEY,
                cedula VARCHAR(20) UNIQUE NOT NULL,
                nombres VARCHAR(255),
                apellidos VARCHAR(255),
                gerencia VARCHAR(255),
                cargo VARCHAR(255),
                fecha_llenado TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # NEW TABLE: 'solicitudes_asistencia' for frontend notifications
        cur.execute("""
            CREATE TABLE IF NOT EXISTS solicitudes_asistencia (
                id_solicitud SERIAL PRIMARY KEY,
                cedula VARCHAR(20) NOT NULL,
                nombres VARCHAR(255) NOT NULL,
                apellidos VARCHAR(255) NOT NULL,
                gerencia_general VARCHAR(255),
                fecha_resolucion DATE,
                status VARCHAR(50) DEFAULT 'pendiente', -- 'pendiente', 'resuelto'
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # NEW TABLE: 'precarga_personal' for pre-loading basic employee info
        cur.execute("""
            CREATE TABLE IF NOT EXISTS precarga_personal (
                id_precarga SERIAL PRIMARY KEY,
                cedula VARCHAR(20) UNIQUE NOT NULL,
                nombres VARCHAR(255) NOT NULL,
                apellidos VARCHAR(255) NOT NULL,
                gerencia_general VARCHAR(255),
                gerencia_especifica VARCHAR(255),
                cargo VARCHAR(255),
                creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actualizado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS empleados (
                id_empleado SERIAL PRIMARY KEY,
                cedula VARCHAR(20) UNIQUE NOT NULL,
                nombres VARCHAR(255) NOT NULL,
                apellidos VARCHAR(255) NOT NULL,
                -- ...otros campos...
                cargo_propuesto VARCHAR(255), -- <--- NUEVO CAMPO
                creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actualizado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS referencias_personales (
            id_referencia SERIAL PRIMARY KEY,
            id_empleado INTEGER NOT NULL REFERENCES empleados(id_empleado) ON DELETE CASCADE,
            nombre VARCHAR(255) NOT NULL,
            telefono VARCHAR(50) NOT NULL,
            parentesco VARCHAR(100) NOT NULL,
            direccion VARCHAR(255),
            ocupacion VARCHAR(100),
            creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS gerencias_generales (
            id_gerencia_general SERIAL PRIMARY KEY,
            nombre VARCHAR(255) NOT NULL UNIQUE
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS gerencias_especificas (
                id_gerencia_especifica SERIAL PRIMARY KEY,
                nombre VARCHAR(255) NOT NULL,
                id_gerencia_general INTEGER NOT NULL REFERENCES gerencias_generales(id_gerencia_general) ON DELETE CASCADE,
                UNIQUE(nombre, id_gerencia_general)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS cargos (
                id_cargo SERIAL PRIMARY KEY,
                nombre VARCHAR(255) NOT NULL,
                id_gerencia_especifica INTEGER NOT NULL REFERENCES gerencias_especificas(id_gerencia_especifica) ON DELETE CASCADE,
                UNIQUE(nombre, id_gerencia_especifica)
            );
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS declaraciones_ruta (
                id_declaracion SERIAL PRIMARY KEY,
                id_empleado INTEGER NOT NULL REFERENCES empleados(id_empleado) ON DELETE CASCADE,
                fecha_declaracion DATE DEFAULT CURRENT_DATE,
                origen VARCHAR(255) NOT NULL,
                destino VARCHAR(255) NOT NULL,
                transporte_ida VARCHAR(100),
                transporte_ida_otro TEXT,
                hora_salida_ida TIMESTAMP,
                transporte_regreso VARCHAR(100),
                transporte_regreso_otro TEXT,
                ruta_alterna_requerida BOOLEAN DEFAULT FALSE,
                ruta_alterna_descripcion TEXT,
                creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actualizado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(id_empleado, fecha_declaracion)
            );
        """)
        # Add a column to the 'empleados' table to track if the route declaration has been filled
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='empleados' AND column_name='declaracion_ruta_llena') THEN
                    ALTER TABLE empleados ADD COLUMN declaracion_ruta_llena BOOLEAN DEFAULT FALSE;
                END IF;
            END
            $$;
        """)

        conn.commit()
        print("Tablas verificadas/creadas exitosamente.")
       
        # Insert initial HR/Admin user if 'usuarios' table is empty
        cur.execute("SELECT COUNT(*) FROM usuarios;")
        if cur.fetchone()[0] == 0:
            print("Inserting initial user data...")
            hashed_password_admin = generate_password_hash('C11P2025.')
            hashed_password_rrhh = generate_password_hash('rrhh2025.') # Example password for HR
            cur.execute(
                """
                INSERT INTO usuarios (usuario, clave, privilegio) VALUES
                ('V-12345678', %s, 'Admin'),
                ('V-87654321', %s, 'Recursos Humanos');
                """,
                (hashed_password_admin, hashed_password_rrhh)
            )
            conn.commit()
            print("Initial user data inserted.")

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        app.logger.error(f"Error al crear/verificar tablas o poblar datos: {e}")
    except Exception as e:
        app.logger.error(f"Error inesperado en crear_tablas_si_no_existen: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# Execute table creation on application startup
with app.app_context():
    crear_tablas_si_no_existen()

# Helper function to convert CSV values to appropriate Python types (and None for empty strings)
def _process_value(value, target_type=str):
    """
    Converts empty strings to None and attempts type conversion.
    """
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None # Treat empty strings as None

    if value is None:
        return None

    try:
        if target_type == int:
            return int(value)
        elif target_type == float:
            return float(value)
        elif target_type == bool:
            return str(value).lower() == 'true'
        elif target_type == datetime.date:
            return datetime.strptime(value, '%Y-%m-%d').date()
        elif target_type == datetime: # For timestamps
            return datetime.fromisoformat(value) # Assumes ISO format from export
        elif target_type == dict or target_type == list: # For JSONB
            # If the value is already a dict/list (e.g. if passed directly from a JSON source), return it
            if isinstance(value, (dict, list)):
                return value
            return json.loads(value)
        else: # Default to string
            return str(value)
    except (ValueError, TypeError, json.JSONDecodeError) as e:
        app.logger.warning(f"Could not convert value '{value}' to type {target_type} for DB. Error: {e}. Returning None.")
        return None # Return None if conversion fails

# Helper function to handle dynamic fields update (education, experience, courses, languages, skills)
def actualizar_campos_dinamicos(cursor, id_empleado, tipo_campo, registros):
    """
    Updates dynamic fields (education, work experience, courses, languages, skills)
    for a given employee, deleting existing records and inserting new ones.
    """
    if tipo_campo == 'education':
        cursor.execute("DELETE FROM educacion WHERE id_empleado = %s;", (id_empleado,))
        for reg in registros:
            nombre_carrera = _process_value(reg.get('nombre_carrera'), str)
            nivel_educativo = _process_value(reg.get('nivel_educativo'), str)
            fecha_graduacion = _process_value(reg.get('fecha_graduacion'), datetime.date)
            tipo_estudio = _process_value(reg.get('tipo_estudio'), str)
            
            if nombre_carrera and nivel_educativo and tipo_estudio: # Ensure core fields are present
                cursor.execute(
                    """
                    INSERT INTO educacion (id_empleado, nombre_carrera, nivel_educativo, fecha_graduacion, tipo_estudio)
                    VALUES (%s, %s, %s, %s, %s);
                    """,
                    (id_empleado, nombre_carrera, nivel_educativo, fecha_graduacion, tipo_estudio)
                )
    elif tipo_campo == 'experience':
        cursor.execute("DELETE FROM experiencia_laboral WHERE id_empleado = %s;", (id_empleado,))
        for reg in registros:
            nombre_empresa = _process_value(reg.get('nombre_empresa'), str)
            fecha_ingreso = _process_value(reg.get('fecha_ingreso'), datetime.date)
            fecha_fin = _process_value(reg.get('fecha_fin'), datetime.date)
            dependencia_organizativa = _process_value(reg.get('dependencia_organizativa'), str)
            cargo = _process_value(reg.get('cargo'), str)
            telefono = _process_value(reg.get('telefono'), str)
            ultimo_sueldo = _process_value(reg.get('ultimo_sueldo'), float)
            descripcion = _process_value(reg.get('descripcion'), str)
            
            if nombre_empresa and cargo: # Ensure core fields are present
                cursor.execute(
                    """
                    INSERT INTO experiencia_laboral (id_empleado, nombre_empresa, fecha_ingreso, fecha_fin, dependencia_organizativa, cargo, telefono, ultimo_sueldo, descripcion)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (id_empleado, nombre_empresa, fecha_ingreso, fecha_fin,
                     dependencia_organizativa, cargo, telefono, ultimo_sueldo, descripcion)
                )
    elif tipo_campo == 'courses':
        cursor.execute("DELETE FROM cursos WHERE id_empleado = %s;", (id_empleado,))
        for reg in registros:
            nombre_curso = _process_value(reg.get('nombre_curso'), str)
            institucion = _process_value(reg.get('institucion'), str)
            fecha_inicio = _process_value(reg.get('fecha_inicio'), datetime.date)
            fecha_fin = _process_value(reg.get('fecha_fin'), datetime.date)
            duracion_horas = _process_value(reg.get('duracion_horas'), int)
            
            if nombre_curso and institucion: # Ensure core fields are present
                cursor.execute(
                    """
                    INSERT INTO cursos (id_empleado, nombre_curso, institucion, fecha_inicio, fecha_fin, duracion_horas)
                    VALUES (%s, %s, %s, %s, %s, %s);
                    """,
                    (id_empleado, nombre_curso, institucion, fecha_inicio, fecha_fin, duracion_horas)
                )
    elif tipo_campo == 'languages':
        cursor.execute("DELETE FROM idiomas WHERE id_empleado = %s;", (id_empleado,))
        for reg in registros:
            idioma = _process_value(reg.get('idioma'), str)
            nivel = _process_value(reg.get('nivel'), str)
            
            if idioma and nivel: # Ensure core fields are present
                cursor.execute(
                    """
                    INSERT INTO idiomas (id_empleado, idioma, nivel)
                    VALUES (%s, %s, %s);
                    """,
                    (id_empleado, idioma, nivel)
                )
    elif tipo_campo == 'skills':
        cursor.execute("DELETE FROM habilidades WHERE id_empleado = %s;", (id_empleado,))
        for reg in registros:
            habilidad = _process_value(reg.get('habilidad'), str)
            
            if habilidad: # Ensure core field is present
                cursor.execute(
                    """
                    INSERT INTO habilidades (id_empleado, habilidad)
                    VALUES (%s, %s);
                    """,
                    (id_empleado, habilidad)
                )

# --- Route to serve the HTML page ---
@app.route('/api/auth/login', methods=['POST'])
def login():
    """
    Endpoint para autenticación de usuarios.
    Espera JSON: { "cedula": "...", "clave": "..." }
    """
    data = request.get_json()
    cedula = data.get('cedula')
    clave = data.get('clave')

    if not cedula or not clave:
        return jsonify({"error": "Cédula y clave son requeridas."}), 400

    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("SELECT usuario, clave, privilegio FROM usuarios WHERE usuario = %s;", (cedula,))
        user = cur.fetchone()
        if not user:
            return jsonify({"error": "Usuario no encontrado."}), 401

        usuario_db, clave_hash, privilegio = user
        if not check_password_hash(clave_hash, clave):
            return jsonify({"error": "Clave incorrecta."}), 401

        # Puedes devolver más datos si lo deseas
        return jsonify({
            "cedula": usuario_db,
            "privilegio": privilegio
        }), 200
    except Exception as e:
        app.logger.error(f"Error en login: {e}")
        return jsonify({"error": "Error interno del servidor."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/rrhh/empleados', methods=['POST'])
def rrhh_empleados():
    """
    Devuelve la lista de empleados para Recursos Humanos.
    Requiere privilegio de Admin o RRHH.
    """
    data = request.get_json()
    cedula = data.get('cedula')
    privilegio = data.get('privilegio')
    if privilegio not in ['Admin', 'Recursos Humanos']:
        return jsonify({"error": "No autorizado."}), 403

    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("SELECT cedula, nombres, apellidos, gerencia_general, gerencia_especifica, cargo FROM empleados;")
        rows = cur.fetchall()
        empleados = [
            {
                "cedula": row[0],
                "nombres": row[1],
                "apellidos": row[2],
                "gerencia_general": row[3],
                "gerencia_especifica": row[4],
                "cargo": row[5]
            }
            for row in rows
        ]
        return jsonify(empleados), 200
    except Exception as e:
        app.logger.error(f"Error al obtener empleados para RRHH: {e}")
        return jsonify({"error": "Error interno del servidor."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

# Supón que este es tu endpoint para guardar empleados (POST o PUT)
@app.route('/api/empleados', methods=['POST'])
def guardar_empleado():
    """
    Guarda o actualiza los datos de un empleado, incluyendo referencias personales y cargo propuesto.
    Espera JSON con los campos del empleado, referencias_personales (lista de dicts) y opcionalmente cargo_propuesto.
    """
    data = request.get_json()
    cedula = data.get('cedula')
    if not cedula:
        return jsonify({"error": "Cédula es requerida."}), 400

    referencias = data.get('referencias_personales', [])
    app.logger.info(f"REFERENCIAS RECIBIDAS: {referencias} (tipo: {type(referencias)})")
    cargo_propuesto = data.get('cargo_propuesto')
    telefonos_emergencia = data.get('telefonos_emergencia', [])

    # Validar mínimo 2 referencias personales
    if not isinstance(referencias, list) or len(referencias) < 2:
        return jsonify({"error": "Debe ingresar al menos 2 referencias personales."}), 400

    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        # 1. Insertar o actualizar empleado (agrega aquí todos los campos relevantes)
        campos = [
            'cedula', 'nombres', 'apellidos', 'nacionalidad', 'lugar_nacimiento', 'fecha_nacimiento', 'edad',
            'estado_civil', 'sexo', 'mano_dominante', 'num_hijos', 'hijos_edades', 'telefono_habitacion',
            'telefono_personal', 'profesion', 'impedimento_medico_fisico',
            'talla_camisa', 'talla_pantalon', 'talla_calzado', 'condicion_habitacion', 'correo_electronico',
            'gerencia_general', 'gerencia_especifica', 'cargo', 'esta_estudiando_actualmente',
            'carrera_actual', 'ano_actual', 'turno_estudio', 'cargo_propuesto'
        ]

        # Obtener id_empleado
        cur.execute("SELECT id_empleado FROM empleados WHERE cedula = %s;", (cedula,))
        id_empleado = cur.fetchone()[0]

        if 'email' in data and data['email']:
            data['correo_electronico'] = data['email']

        if not data.get('correo_electronico'):
            cur.execute("SELECT correo_electronico FROM empleados WHERE cedula = %s;", (cedula,))
            row = cur.fetchone()
            if row and row[0]:
                data['correo_electronico'] = row[0]

        valores = [data.get(campo) for campo in campos]

        # ... después de obtener id_empleado ...

        # Guardar o actualizar dirección del empleado
        direccion = data.get('direccion', {})

        if direccion:
            id_estado = direccion.get('id_estado')
            id_municipio = direccion.get('id_municipio')
            id_parroquia = direccion.get('id_parroquia')
            id_ciudad = direccion.get('id_ciudad')
            localidad = direccion.get('localidad')
            nombre_localidad = direccion.get('nombre_localidad')
            tipo_inmueble = direccion.get('tipo_inmueble')
            numero_casa = direccion.get('numero_casa')
            edificio_bloque_torre = direccion.get('edificio_bloque_torre')
            piso = direccion.get('piso')
            puerta = direccion.get('puerta')
            direccion_detallada = direccion.get('direccion_detallada')
            zona_postal = direccion.get('zona_postal')
            condicion_habitacion = direccion.get('condicion_habitacion')

            cur.execute("SELECT id_direccion FROM direcciones WHERE id_empleado = %s;", (id_empleado,))
            existe = cur.fetchone()

            if existe:
                cur.execute("""
                    UPDATE direcciones
                    SET id_estado = %s,
                        id_municipio = %s,
                        id_parroquia = %s,
                        id_ciudad = %s,
                        localidad = %s,
                        nombre_localidad = %s,
                        tipo_inmueble = %s,
                        numero_casa = %s,
                        edificio_bloque_torre = %s,
                        piso = %s,
                        puerta = %s,
                        direccion_detallada = %s,
                        zona_postal = %s,
                        condicion_habitacion = %s,
                        actualizado_en = CURRENT_TIMESTAMP
                    WHERE id_empleado = %s;
                """, (
                    id_estado, id_municipio, id_parroquia, id_ciudad, localidad, nombre_localidad, tipo_inmueble,
                    numero_casa, edificio_bloque_torre, piso, puerta, direccion_detallada, zona_postal,
                    condicion_habitacion, id_empleado
                ))
            else:
                cur.execute("""
                    INSERT INTO direcciones (
                        id_empleado, id_estado, id_municipio, id_parroquia, id_ciudad,
                        localidad, nombre_localidad, tipo_inmueble, numero_casa, edificio_bloque_torre,
                        piso, puerta, direccion_detallada, zona_postal, condicion_habitacion
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                    id_empleado, id_estado, id_municipio, id_parroquia, id_ciudad, localidad, nombre_localidad,
                    tipo_inmueble, numero_casa, edificio_bloque_torre, piso, puerta, direccion_detallada,
                    zona_postal, condicion_habitacion
                ))

        if 'courses' in data and not data.get('cursos'):
            data['cursos'] = data['courses']

        # Guardar cursos (elimina los anteriores y agrega los nuevos)
        cursos = data.get('cursos', [])
        cur.execute("DELETE FROM cursos WHERE id_empleado = %s;", (id_empleado,))
        for curso in cursos:
            cur.execute("""
                INSERT INTO cursos (id_empleado, nombre_curso, institucion, fecha_inicio, fecha_fin, duracion_horas)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                id_empleado,
                curso.get('nombre_curso', ''),
                curso.get('institucion', ''),
                curso.get('fecha_inicio', None),
                curso.get('fecha_fin', None),
                curso.get('duracion_horas', None)
            ))

        # Teléfonos de emergencia
        cur.execute("DELETE FROM Telefono_Emergencia WHERE id_personal = %s;", (id_empleado,))
        telefonos_emergencia = data.get('telefonos_emergencia', [])
        for tel in telefonos_emergencia:
            nombre = tel.get('nombre')
            telefono = tel.get('telefono')
            parentesco = tel.get('parentesco')
            if nombre and telefono:
                cur.execute(
                    "INSERT INTO Telefono_Emergencia (id_personal, nombre, telefono, parentesco) VALUES (%s, %s, %s, %s);",
                    (id_empleado, nombre, telefono, parentesco)
                )

        # Hijos
        cur.execute("DELETE FROM hijos WHERE id_personal = %s;", (id_empleado,))
        hijos_edades = data.get('hijos_edades', [])
        for edad in hijos_edades:
            if isinstance(edad, int) and edad > 0:
                cur.execute(
                    "INSERT INTO hijos (id_personal, edad) VALUES (%s, %s);",
                    (id_empleado, edad)
                )

        # Referencias personales
        cur.execute("DELETE FROM referencias_personales WHERE id_empleado = %s;", (id_empleado,))
        for ref in referencias:
            cur.execute("""
                INSERT INTO referencias_personales (id_empleado, nombre, telefono, parentesco, direccion, ocupacion)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                id_empleado,
                ref.get('nombre', ''),
                ref.get('telefono', ''),
                ref.get('parentesco', ''),
                ref.get('direccion', ''),
                ref.get('ocupacion', '')
            ))

        # 3. Eliminar de precarga_personal si existe
        cur.execute("DELETE FROM precarga_personal WHERE cedula = %s;", (cedula,))

        conn.commit()
        return jsonify({"message": "Empleado guardado y referencias personales actualizadas."}), 200

    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error al guardar empleado: {e}")
        return jsonify({"error": "Error al guardar empleado."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/empleados/actualizar_curriculum', methods=['POST'])
def actualizar_curriculum():
    data = request.get_json()
    cedula = data.get('cedula')
    if not cedula:
        return jsonify({"error": "Cédula es requerida."}), 400

    referencias = data.get('referencias_personales', [])
    app.logger.info(f"REFERENCIAS RECIBIDAS: {referencias} (tipo: {type(referencias)})")
    cargo_propuesto = data.get('cargo_propuesto')
    telefonos_emergencia = data.get('telefonos_emergencia', [])

    # Validar mínimo 2 referencias personales
    if not isinstance(referencias, list) or len(referencias) < 2:
        return jsonify({"error": "Debe ingresar al menos 2 referencias personales."}), 400

    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        # 1. Actualizar o insertar empleado (agrega aquí todos los campos relevantes)
        campos = [
            'cedula', 'nombres', 'apellidos', 'nacionalidad', 'lugar_nacimiento', 'fecha_nacimiento', 'edad',
            'estado_civil', 'sexo', 'mano_dominante', 'num_hijos', 'hijos_edades', 'telefono_habitacion',
            'telefono_personal', 'profesion', 'impedimento_medico_fisico',
            'talla_camisa', 'talla_pantalon', 'talla_calzado', 'condicion_habitacion', 'correo_electronico',
            'gerencia_general', 'gerencia_especifica', 'cargo', 'esta_estudiando_actualmente',
            'carrera_actual', 'ano_actual', 'turno_estudio', 'cargo_propuesto'
        ]

        # Obtener id_empleado
        cur.execute("SELECT id_empleado FROM empleados WHERE cedula = %s;", (cedula,))
        id_empleado = cur.fetchone()[0]

        if 'email' in data and data['email']:
            data['correo_electronico'] = data['email']

        if not data.get('correo_electronico'):
            cur.execute("SELECT correo_electronico FROM empleados WHERE cedula = %s;", (cedula,))
            row = cur.fetchone()
            if row and row[0]:
                data['correo_electronico'] = row[0]

        valores = [data.get(campo) for campo in campos]

        # Si hijos_edades es lista/dict, conviértelo a JSON
        import json
        hijos_edades = data.get('hijos_edades')
        if isinstance(hijos_edades, (list, dict)):
            valores[campos.index('hijos_edades')] = json.dumps(hijos_edades)

        # Upsert empleado
        update_set = ', '.join([f"{campo} = EXCLUDED.{campo}" for campo in campos if campo != 'cedula'])
        cur.execute(f"""
            INSERT INTO empleados ({', '.join(campos)})
            VALUES ({', '.join(['%s'] * len(campos))})
            ON CONFLICT (cedula) DO UPDATE SET {update_set}, formulario_lleno = TRUE, actualizado_en = CURRENT_TIMESTAMP;
        """, valores)

        # Obtener id_empleado
        cur.execute("SELECT id_empleado FROM empleados WHERE cedula = %s;", (cedula,))
        id_empleado = cur.fetchone()[0]

                # Guardar o actualizar dirección del empleado
        direccion = data.get('direccion', {})

        if direccion:
            id_estado = direccion.get('id_estado')
            id_municipio = direccion.get('id_municipio')
            id_parroquia = direccion.get('id_parroquia')
            id_ciudad = direccion.get('id_ciudad')
            localidad = direccion.get('localidad')
            nombre_localidad = direccion.get('nombre_localidad')
            tipo_inmueble = direccion.get('tipo_inmueble')
            numero_casa = direccion.get('numero_casa')
            edificio_bloque_torre = direccion.get('edificio_bloque_torre')
            piso = direccion.get('piso')
            puerta = direccion.get('puerta')
            direccion_detallada = direccion.get('direccion_detallada')
            zona_postal = direccion.get('zona_postal')
            condicion_habitacion = direccion.get('condicion_habitacion')

            cur.execute("SELECT id_direccion FROM direcciones WHERE id_empleado = %s;", (id_empleado,))
            existe = cur.fetchone()

            if existe:
                cur.execute("""
                    UPDATE direcciones
                    SET id_estado = %s,
                        id_municipio = %s,
                        id_parroquia = %s,
                        id_ciudad = %s,
                        localidad = %s,
                        nombre_localidad = %s,
                        tipo_inmueble = %s,
                        numero_casa = %s,
                        edificio_bloque_torre = %s,
                        piso = %s,
                        puerta = %s,
                        direccion_detallada = %s,
                        zona_postal = %s,
                        condicion_habitacion = %s,
                        actualizado_en = CURRENT_TIMESTAMP
                    WHERE id_empleado = %s;
                """, (
                    id_estado, id_municipio, id_parroquia, id_ciudad, localidad, nombre_localidad, tipo_inmueble,
                    numero_casa, edificio_bloque_torre, piso, puerta, direccion_detallada, zona_postal,
                    condicion_habitacion, id_empleado
                ))
            else:
                cur.execute("""
                    INSERT INTO direcciones (
                        id_empleado, id_estado, id_municipio, id_parroquia, id_ciudad,
                        localidad, nombre_localidad, tipo_inmueble, numero_casa, edificio_bloque_torre,
                        piso, puerta, direccion_detallada, zona_postal, condicion_habitacion
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                    id_empleado, id_estado, id_municipio, id_parroquia, id_ciudad, localidad, nombre_localidad,
                    tipo_inmueble, numero_casa, edificio_bloque_torre, piso, puerta, direccion_detallada,
                    zona_postal, condicion_habitacion
                ))

        app.logger.info(f"Guardando dirección: {direccion}")


        # ...después de obtener id_empleado...

        # Justo antes de: cursos = data.get('cursos', [])
        if 'courses' in data and not data.get('cursos'):
            data['cursos'] = data['courses']

        # Guardar cursos (elimina los anteriores y agrega los nuevos)
        cursos = data.get('cursos', [])
        cur.execute("DELETE FROM cursos WHERE id_empleado = %s;", (id_empleado,))
        for curso in cursos:
            cur.execute("""
                INSERT INTO cursos (id_empleado, nombre_curso, institucion, fecha_inicio, fecha_fin, duracion_horas)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                id_empleado,
                curso.get('nombre_curso', ''),
                curso.get('institucion', ''),
                curso.get('fecha_inicio', None),
                curso.get('fecha_fin', None),
                curso.get('duracion_horas', None)
            ))

        # Guardar habilidades (elimina las anteriores y agrega las nuevas)
        habilidades = data.get('skills', [])
        cur.execute("DELETE FROM habilidades WHERE id_empleado = %s;", (id_empleado,))
        for habilidad in habilidades:
            cur.execute("""
                INSERT INTO habilidades (id_empleado, habilidad)
                VALUES (%s, %s)
            """, (
                id_empleado,
                habilidad.get('habilidad', '')
            ))

        # Guardar idiomas
        # ...existing code after obtener id_empleado...

        # Guardar idiomas (elimina los anteriores y agrega los nuevos)
        idiomas = data.get('languages', [])
        cur.execute("DELETE FROM idiomas WHERE id_empleado = %s;", (id_empleado,))
        for idioma in idiomas:
            cur.execute("""
                INSERT INTO idiomas (id_empleado, idioma, nivel)
                VALUES (%s, %s, %s)
            """, (
                id_empleado,
                idioma.get('idioma', ''),
                idioma.get('nivel', '')
            ))

        # 2. Actualizar referencias personales (eliminar y volver a insertar)
        cur.execute("DELETE FROM referencias_personales WHERE id_empleado = %s;", (id_empleado,))
        for ref in referencias:
            cur.execute("""
                INSERT INTO referencias_personales (id_empleado, nombre, telefono, parentesco, direccion, ocupacion)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                id_empleado,
                ref.get('nombre', ''),
                ref.get('telefono', ''),
                ref.get('parentesco', ''),
                ref.get('direccion', ''),
                ref.get('ocupacion', '')
            ))

        # Teléfonos de emergencia
        cur.execute("DELETE FROM Telefono_Emergencia WHERE id_personal = %s;", (id_empleado,))
        telefonos_emergencia = data.get('telefonos_emergencia', [])
        for tel in telefonos_emergencia:
            nombre = tel.get('nombre')
            telefono = tel.get('telefono')
            parentesco = tel.get('parentesco')
            if nombre and telefono:
                cur.execute(
                    "INSERT INTO Telefono_Emergencia (id_personal, nombre, telefono, parentesco) VALUES (%s, %s, %s, %s);",
                    (id_empleado, nombre, telefono, parentesco)
                )

        # Hijos
        cur.execute("DELETE FROM hijos WHERE id_personal = %s;", (id_empleado,))
        hijos_edades = data.get('hijos_edades', [])
        for edad in hijos_edades:
            if isinstance(edad, int) and edad > 0:
                cur.execute(
                    "INSERT INTO hijos (id_personal, edad) VALUES (%s, %s);",
                    (id_empleado, edad)
                )
        # ...después de guardar el currículum y obtener los datos del empleado...
        cur.execute("SELECT nombres, apellidos, gerencia_general, cargo FROM empleados WHERE cedula = %s;", (cedula,))
        row = cur.fetchone()
        if row:
            nombres, apellidos, gerencia, cargo = row
            # Verifica si ya existe el registro
            cur.execute("SELECT * FROM realizados WHERE cedula = %s;", (cedula,))
            if not cur.fetchone():
                cur.execute("""
                    INSERT INTO realizados (cedula, nombres, apellidos, gerencia, cargo, fecha_llenado)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                """, (cedula, nombres, apellidos, gerencia, cargo))
                conn.commit()

        # 4. Eliminar de precarga_personal si existe
        cur.execute("DELETE FROM precarga_personal WHERE cedula = %s;", (cedula,))

        conn.commit()
        return jsonify({"mensaje": "Currículum actualizado con éxito."}), 200

    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error al actualizar currículum: {e}")
        return jsonify({"error": "Error al actualizar currículum."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/')
def index():
    """
    Serves the main HTML application file.
    NOTE: For this to work, ensure your 'intranet.html' file
    is in a folder named 'templates' in the same directory as this script.
    """
    return render_template('intranet.html')

@app.route('/api/dashboard/language_proficiency', methods=['GET'])
def language_proficiency_alias():
    """
    Devuelve la cantidad de empleados por idioma y nivel.
    """
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT idioma, nivel, COUNT(*) as count
            FROM idiomas
            GROUP BY idioma, nivel
            ORDER BY idioma, nivel;
        """)
        data = [{"idioma": row[0], "nivel": row[1], "count": row[2]} for row in cur.fetchall()]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al obtener idiomas por nivel: {e}")
        return jsonify({"error": "Error al obtener datos de idiomas."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/dashboard/skills_overview', methods=['GET'])
def skills_overview_alias():
    """
    Devuelve la cantidad de empleados por habilidad.
    """
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT habilidad, COUNT(*) as count
            FROM habilidades
            GROUP BY habilidad
            ORDER BY count DESC, habilidad;
        """)
        data = [{"habilidad": row[0], "count": row[1]} for row in cur.fetchall()]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al obtener habilidades: {e}")
        return jsonify({"error": "Error al obtener datos de habilidades."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/dashboard/marital_status_distribution', methods=['GET'])
def marital_status_distribution_alias():
    """
    Devuelve la cantidad de empleados por estado civil.
    """
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT estado_civil, COUNT(*) as count
            FROM empleados
            GROUP BY estado_civil
            ORDER BY estado_civil;
        """)
        data = [{"estado_civil": row[0], "count": row[1]} for row in cur.fetchall()]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al obtener distribución por estado civil: {e}")
        return jsonify({"error": "Error al obtener datos de estado civil."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

# --- API Routes for Locations ---
@app.route('/api/ubicaciones/localidades', methods=['GET'])
def get_ubicaciones_localidad():
    """
    Devuelve la lista de localidades para el dropdown de dirección.
    """
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("SELECT id, nombre FROM ubicaciones_localidad ORDER BY nombre;")
        rows = cur.fetchall()
        data = [{"id": row[0], "nombre": row[1]} for row in rows]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al consultar localidades: {e}")
        return jsonify({"error": "Error al consultar localidades"}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/ubicaciones/inmuebles', methods=['GET'])
def get_ubicaciones_inmueble():
    """
    Devuelve la lista de tipos de inmueble para el dropdown de dirección.
    """
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("SELECT id, nombre, tipo FROM ubicaciones_inmueble ORDER BY nombre;")
        rows = cur.fetchall()
        data = [{"id": row[0], "nombre": row[1], "tipo": row[2]} for row in rows]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al consultar inmuebles: {e}")
        return jsonify({"error": "Error al consultar inmuebles"}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/ubicaciones/estados', methods=['GET'])
def get_ubicaciones_estados():
    """
    Devuelve la lista de estados.
    """
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("SELECT id_estado, nombre FROM ubicaciones_estados ORDER BY nombre;")
        rows = cur.fetchall()
        data = [{"id": row[0], "nombre": row[1]} for row in rows]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al consultar estados: {e}")
        return jsonify({"error": "Error al consultar estados"}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/ubicaciones/ciudades', methods=['GET'])
def get_ubicaciones_ciudades():
    """
    Devuelve la lista de ciudades para un estado dado.
    """
    id_estado = request.args.get('id_estado')
    if not id_estado:
        return jsonify([]), 200
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("SELECT id_ciudad, nombre FROM ubicaciones_ciudades WHERE id_estado = %s ORDER BY nombre;", (id_estado,))
        rows = cur.fetchall()
        data = [{"id": row[0], "nombre": row[1]} for row in rows]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al consultar ciudades: {e}")
        return jsonify({"error": "Error al consultar ciudades"}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/ubicaciones/municipios', methods=['GET'])
def get_ubicaciones_municipios():
    """
    Devuelve la lista de municipios para una ciudad dada.
    """
    id_ciudad = request.args.get('id_ciudad')
    if not id_ciudad:
        return jsonify([]), 200
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("SELECT id_municipio, nombre FROM ubicaciones_municipios WHERE id_ciudad = %s ORDER BY nombre;", (id_ciudad,))
        rows = cur.fetchall()
        data = [{"id": row[0], "nombre": row[1]} for row in rows]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al consultar municipios: {e}")
        return jsonify({"error": "Error al consultar municipios"}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/ubicaciones/parroquias', methods=['GET'])
def get_ubicaciones_parroquias():
    """
    Devuelve la lista de parroquias para un municipio dado.
    """
    id_municipio = request.args.get('id_municipio')
    if not id_municipio:
        return jsonify([]), 200
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("SELECT id_parroquia, nombre FROM ubicaciones_parroquias WHERE id_municipio = %s ORDER BY nombre;", (id_municipio,))
        rows = cur.fetchall()
        data = [{"id": row[0], "nombre": row[1]} for row in rows]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al consultar parroquias: {e}")
        return jsonify({"error": "Error al consultar parroquias"}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

def enviar_correo(destinatario, asunto, mensaje):
    """
    Envía un correo real usando la configuración SMTP definida.
    """
    try:
        msg = MIMEText(mensaje, "html", "utf-8")
        msg['Subject'] = asunto
        msg['From'] = EMAIL_SENDER
        msg['To'] = destinatario

        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, [destinatario], msg.as_string())
        print(f"Correo enviado a {destinatario}: {asunto}")
        return True
    except Exception as e:
        print(f"Error al enviar correo: {e}")
        return False

@app.route('/api/auth/recuperar_usuario', methods=['POST'])
def recuperar_usuario():
    data = request.get_json()
    cedula = data.get('cedula', '').strip()
    email = data.get('email', '').strip().lower()
    if not cedula or not email:
        return jsonify({"error": "Debe proporcionar cédula y correo electrónico."}), 400

    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        # Validar que la cédula y el correo coincidan en empleados
        cur.execute("SELECT correo_electronico FROM empleados WHERE cedula = %s;", (cedula,))
        row = cur.fetchone()
        if not row or row[0].lower() != email:
            return jsonify({"error": "La cédula y el correo no coinciden."}), 404

        # Buscar usuario en la tabla usuarios
        cur.execute("SELECT usuario FROM usuarios WHERE usuario = %s;", (cedula,))
        user_row = cur.fetchone()
        if not user_row:
            return jsonify({"error": "No existe usuario registrado con esa cédula."}), 404

        asunto = "Recuperación de Usuario"
        mensaje = f"Su usuario registrado es: {cedula}\nSi no solicitó esta recuperación, ignore este mensaje."
        enviar_correo(email, asunto, mensaje)
        return jsonify({"mensaje": "Se ha enviado su usuario al correo registrado."}), 200
    except Exception as e:
        return jsonify({"error": f"Error interno: {e}"}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/auth/recuperar_clave', methods=['POST'])
def recuperar_clave():
    data = request.get_json()
    cedula = data.get('cedula', '').strip()
    email = data.get('email', '').strip().lower()
    usuario = data.get('usuario', '').strip()
    if not cedula or not email or not usuario:
        return jsonify({"error": "Debe proporcionar cédula, usuario y correo electrónico."}), 400

    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        # Validar que la cédula y el correo coincidan en empleados
        cur.execute("SELECT correo_electronico FROM empleados WHERE cedula = %s;", (cedula,))
        row = cur.fetchone()
        if not row or row[0].lower() != email:
            return jsonify({"error": "La cédula y el correo no coinciden."}), 404

        # Validar que el usuario existe y corresponde a la cédula
        cur.execute("SELECT usuario FROM usuarios WHERE usuario = %s;", (usuario,))
        user_row = cur.fetchone()
        if not user_row or user_row[0] != cedula:
            return jsonify({"error": "El usuario no corresponde a la cédula."}), 404

        # Genera una clave temporal segura
        clave_temporal = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        cur.execute("UPDATE usuarios SET clave = %s WHERE usuario = %s;", (clave_temporal, usuario))
        conn.commit()
        asunto = "Recuperación de Clave"
        mensaje = f"Su nueva clave temporal es: {clave_temporal}\nPor favor, cámbiela después de ingresar.\nSi no solicitó esta recuperación, ignore este mensaje."
        enviar_correo(email, asunto, mensaje)
        return jsonify({"mensaje": "Se ha enviado una clave temporal al correo registrado."}), 200
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error": f"Error interno: {e}"}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/auth/verificar_codigo_cambiar_clave', methods=['POST'])
def verificar_codigo_cambiar_clave():
    """
    Verifica el código de correo y, si es válido, genera una clave temporal,
    la hashea y la actualiza en la base de datos, luego la envía al correo.
    Espera JSON: { "cedula": "...", "usuario": "...", "correo": "...", "codigo": "123456" }
    """
    data = request.get_json()
    cedula = data.get('cedula')
    usuario = data.get('usuario')
    correo = data.get('correo') or data.get('email')
    codigo = data.get('codigo')

    if not cedula or not usuario or not correo or not codigo:
        return jsonify({"error": "Todos los campos son requeridos."}), 400

    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        # Verifica que el código sea válido y no expirado
        cur.execute("""
            SELECT codigo_verificacion, codigo_expira_en FROM empleados WHERE cedula = %s;
        """, (cedula,))
        result = cur.fetchone()
        if not result:
            return jsonify({"error": "Empleado no encontrado."}), 404

        codigo_db, expira_en = result
        if not codigo_db or str(codigo_db) != str(codigo):
            return jsonify({"error": "El código es incorrecto."}), 400
        if expira_en and datetime.now() > expira_en:
            return jsonify({"error": "El código ha expirado."}), 400

        # Validar que el usuario existe y corresponde a la cédula
        cur.execute("SELECT usuario FROM usuarios WHERE usuario = %s;", (usuario,))
        user_row = cur.fetchone()
        if not user_row or user_row[0] != cedula:
            return jsonify({"error": "El usuario no corresponde a la cédula."}), 404

        # Genera y hashea la clave temporal
        clave_temporal = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        clave_hash = generate_password_hash(clave_temporal)
        cur.execute("UPDATE usuarios SET clave = %s WHERE usuario = %s;", (clave_hash, usuario))
        conn.commit()

        # Envía la clave temporal al correo
        asunto = "Recuperación de Clave"
        mensaje = f"""
        <p>Su nueva clave temporal es: <strong>{clave_temporal}</strong></p>
        <p>Por favor, cámbiela después de ingresar.</p>
        <p>Si no solicitó esta recuperación, ignore este mensaje.</p>
        """
        enviar_correo(correo, asunto, mensaje)

        return jsonify({"mensaje": "Clave temporal generada y enviada al correo registrado."}), 200
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error": f"Error interno: {e}"}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

# --- New Endpoint: Request Assistance ---
@app.route('/api/solicitar_asistencia', methods=['POST'])
def solicitar_asistencia():
    """
    Endpoint to record an assistance request from an employee.
    """
    data = request.get_json()
    cedula = data.get('cedula').upper() if data.get('cedula') else None
    nombres = data.get('nombres')
    apellidos = data.get('apellidos')
    gerencia_general = data.get('gerencia_general')

    if not all([cedula, nombres, apellidos, gerencia_general]):
        return jsonify({"error": "Todos los campos de personal son requeridos para la solicitud de asistencia."}), 400

    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO solicitudes_asistencia (cedula, nombres, apellidos, gerencia_general, estado)
            VALUES (%s, %s, %s, %s, 'pendiente');
            """,
            (cedula, nombres, apellidos, gerencia_general)
        )
        conn.commit()
        return jsonify({"mensaje": "Solicitud de asistencia enviada."}), 200
    except psycopg2.Error as e:
        if conn: conn.rollback()
        app.logger.error(f"Error de base de datos al solicitar asistencia: {e}")
        return jsonify({"error": "Error al procesar la solicitud de asistencia."}), 500
    except Exception as e:
        app.logger.error(f"Error inesperado al solicitar asistencia: {e}")
        return jsonify({"error": "Error inesperado del servidor al procesar solicitud de asistencia."}), 500
    finally:
        if cur: cur.close()
        if conn: cur.close()

# --- New Endpoint: Get Assistance Requests for HR ---
@app.route('/api/rrhh/assistance_requests', methods=['GET'])
def get_assistance_requests():
    """
    Endpoint to get all pending assistance requests for HR personnel.
    """
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT id_solicitud, cedula, nombres, apellidos, gerencia_general, estado, timestamp
            FROM solicitudes_asistencia
            WHERE estado = 'pendiente'
            ORDER BY timestamp ASC;
            """
        )
        requests_raw = cur.fetchall()
        columnas = [desc[0] for desc in cur.description]
        
        pending_requests = []
        for req_data in requests_raw:
            req_dict = dict(zip(columnas, req_data))
            if 'timestamp' in req_dict and isinstance(req_dict['timestamp'], datetime):
                req_dict['timestamp'] = req_dict['timestamp'].isoformat()
            pending_requests.append(req_dict)
            
        return jsonify(pending_requests), 200
    except Exception as e:
        app.logger.error(f"Error al obtener solicitudes de asistencia: {e}")
        return jsonify({"error": "Error interno del servidor al obtener solicitudes."}), 500
    finally:
        if cur: cur.close()
        if conn: cur.close()

# --- New Endpoint: Resolve Assistance Request ---
# --- Ruta para resolver solicitudes de asistencia ---
# Esta ruta debe estar definida solo UNA vez en tu Backend.txt
# --- Ruta para resolver solicitudes de asistencia ---
@app.route('/api/rrhh/assistance_requests/<string:request_id>/resolve', methods=['PUT'])
def resolve_assistance_request(request_id):
    """
    Marca una solicitud de asistencia específica como 'Resuelta' en la base de datos.
    Valida que el ID de la solicitud sea un número entero.
    """
    conn = None
    cur = None
    try:
        # 1. Validar y convertir request_id a entero
        # Si request_id es "undefined" o no es un número, retornar un error 400.
        try:
            if request_id == 'undefined' or not request_id.isdigit():
                app.logger.error(f"ID de solicitud inválido recibido: '{request_id}'. No es un número válido.")
                return jsonify({"error": "ID de solicitud inválido. Debe ser un número entero."}), 400
            
            request_id_int = int(request_id)
        except ValueError: # Esto en realidad no debería ocurrir con .isdigit() pero es buena práctica
            app.logger.error(f"Error de conversión de ID de solicitud '{request_id}' a entero.")
            return jsonify({"error": "ID de solicitud no válido. Debe ser un número entero."}), 400

        conn = obtener_conexion_db()
        cur = conn.cursor()

        # 2. Actualizar el estado de la solicitud a 'Resuelto'
        # Usamos 'id_solicitud' como nombre de columna, según tu error.
        cur.execute("""
            UPDATE solicitudes_asistencia
            SET estado = 'resuelto', fecha_resolucion = NOW()
            WHERE id_solicitud = %s;
        """, (request_id_int,)) # ¡Importante: usar la versión entera del ID aquí!
        conn.commit()

        if cur.rowcount == 0:
            app.logger.warning(f"Solicitud ID '{request_id}' no encontrada o ya estaba resuelta.")
            return jsonify({"message": f"Solicitud de asistencia con ID '{request_id}' no encontrada o no se realizaron cambios."}), 404
        else:
            app.logger.info(f"Solicitud de asistencia con ID '{request_id}' resuelta exitosamente.")
            return jsonify({"message": f"Solicitud de asistencia con ID '{request_id}' resuelta exitosamente."}), 200

    except Exception as e:
        app.logger.error(f"Error interno del servidor al resolver la solicitud de asistencia ID '{request_id}': {e}")
        return jsonify({"error": "Error interno del servidor al resolver la solicitud de asistencia.", "details": str(e)}), 500
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Dashboard API Endpoints ---

@app.route('/api/dashboard/totals', methods=['GET'])
def get_dashboard_totals():
    """
    Endpoint to get the total number of forms filled and total registered personnel.
    """
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(DISTINCT cedula) FROM realizados;")
        total_filled_forms = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM empleados;")
        total_personnel = cur.fetchone()[0]

        return jsonify({
            "total_filled_forms": total_filled_forms,
            "total_personnel": total_personnel
        }), 200
    except Exception as e:
        app.logger.error(f"Error al obtener totales del dashboard: {e}")
        return jsonify({"error": "Error al obtener totales."}), 500
    finally:
        if cur: cur.close()
        if conn: cur.close()

@app.route('/api/dashboard/filled_forms_count', methods=['GET'])
def filled_forms_count():
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM empleados WHERE formulario_lleno = TRUE;")
        count = cur.fetchone()[0]
        return jsonify({"count": count}), 200
    except Exception as e:
        app.logger.error(f"Error al contar formularios llenados: {e}")
        return jsonify({"error": "Error interno del servidor."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/organizacion/gerencias_generales', methods=['GET'])
def listar_gerencias_generales():
    conn = obtener_conexion_db()
    cur = conn.cursor()
    cur.execute("SELECT id_gerencia_general AS id, nombre FROM gerencias_generales ORDER BY nombre;")
    data = [dict(id=row[0], nombre=row[1]) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return jsonify(data)

@app.route('/api/organizacion/gerencias_especificas', methods=['GET'])
def listar_gerencias_especificas():
    id_gerencia_general = request.args.get('id_gerencia_general')
    conn = obtener_conexion_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT id_gerencia_especifica AS id, nombre 
        FROM gerencias_especificas 
        WHERE id_gerencia_general = %s
        ORDER BY nombre;
    """, (id_gerencia_general,))
    data = [dict(id=row[0], nombre=row[1]) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return jsonify(data)

@app.route('/api/organizacion/cargos', methods=['GET'])
def listar_cargos():
    id_gerencia_especifica = request.args.get('id_gerencia_especifica')
    conn = obtener_conexion_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT id_cargo AS id, nombre 
        FROM cargos 
        WHERE id_gerencia_especifica = %s
        ORDER BY nombre;
    """, (id_gerencia_especifica,))
    data = [dict(id=row[0], nombre=row[1]) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return jsonify(data)

@app.route('/api/dashboard/education_by_gerencia', methods=['GET'])
def get_education_by_gerencia():
    """
    Endpoint to get the educational level by general management.
    """
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                e.gerencia_general,
                ed.nivel_educativo,
                COUNT(DISTINCT e.id_empleado) as count
            FROM empleados e
            JOIN educacion ed ON e.id_empleado = ed.id_empleado
            GROUP BY e.gerencia_general, ed.nivel_educativo
            ORDER BY e.gerencia_general, ed.nivel_educativo;
            """
        )
        results = cur.fetchall()
        columnas = [desc[0] for desc in cur.description]
        data = [dict(zip(columnas, row)) for row in results]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al obtener educación por gerencia: {e}")
        return jsonify({"error": "Error al obtener datos de educación por gerencia."}), 500
    finally:
        if cur: cur.close()
        if conn: cur.close()

@app.route('/api/dashboard/education_by_cargo', methods=['GET'])
def get_education_by_cargo():
    """
    Endpoint to get the educational level by job title.
    """
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                e.cargo,
                ed.nivel_educativo,
                COUNT(DISTINCT e.id_empleado) as count
            FROM empleados e
            JOIN educacion ed ON e.id_empleado = ed.id_empleado
            GROUP BY e.cargo, ed.nivel_educativo
            ORDER BY e.cargo, ed.nivel_educativo;
            """
        )
        results = cur.fetchall()
        columnas = [desc[0] for desc in cur.description]
        data = [dict(zip(columnas, row)) for row in results]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al obtener educación por cargo: {e}")
        return jsonify({"error": "Error al obtener datos de educación por cargo."}), 500
    finally:
        if cur: cur.close()
        if conn: cur.close()

@app.route('/api/dashboard/people_by_state', methods=['GET'])
def get_people_by_state():
    """
    Endpoint to get the number of people by state.
    """
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                us.nombre AS estado,
                COUNT(DISTINCT e.id_empleado) AS count
            FROM empleados e
            LEFT JOIN direcciones d ON e.id_empleado = d.id_empleado
            LEFT JOIN ubicaciones_estados us ON d.id_estado = us.id_estado
            GROUP BY us.nombre
            ORDER BY us.nombre;
            """
        )
        results = cur.fetchall()
        columnas = [desc[0] for desc in cur.description]
        data = [dict(zip(columnas, row)) for row in results]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al obtener personas por estado: {e}")
        return jsonify({"error": "Error al obtener datos de personas por estado."}), 500
    finally:
        if cur: cur.close()
        if conn: cur.close()

@app.route('/api/dashboard/courses_by_gerencia', methods=['GET'])
def get_courses_by_gerencia():
    """
    Endpoint to get the number of people with courses by general management.
    """
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                e.gerencia_general,
                COUNT(DISTINCT e.id_empleado) as count
            FROM empleados e
            JOIN cursos c ON e.id_empleado = c.id_empleado
            GROUP BY e.gerencia_general
            ORDER BY e.gerencia_general;
            """
        )
        results = cur.fetchall()
        columnas = [desc[0] for desc in cur.description]
        data = [dict(zip(columnas, row)) for row in results]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al obtener cursos por gerencia: {e}")
        return jsonify({"error": "Error al obtener datos de cursos por gerencia."}), 500
    finally:
        if cur: cur.close()
        if conn: cur.close()

@app.route('/api/dashboard/survey_status', methods=['GET'])
def get_survey_status():
    """
    Endpoint to get the survey status (if they have filled the form) for each employee.
    Allows filtering by cedula.
    """
    cedula_filter = request.args.get('cedula')
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        sql_query = """
            SELECT
                e.cedula,
                e.nombres,
                e.apellidos,
                e.gerencia_general,
                e.cargo,
                CASE WHEN r.cedula IS NOT NULL THEN TRUE ELSE FALSE END AS has_filled_form
            FROM empleados e
            LEFT JOIN realizados r ON e.cedula = r.cedula
        """
        params = []

        if cedula_filter:
            sql_query += " WHERE e.cedula = %s"
            params.append(cedula_filter)
        
        sql_query += " ORDER BY e.cedula;"

        cur.execute(sql_query, tuple(params))
        employees_raw = cur.fetchall()
        columnas = [desc[0] for desc in cur.description]
        
        survey_status_list = []
        for emp_data in employees_raw:
            emp_dict = dict(zip(columnas, emp_data))
            survey_status_list.append(emp_dict)
            
        return jsonify(survey_status_list), 200
    except Exception as e:
        app.logger.error(f"Error al obtener estado de la encuesta: {e}")
        return jsonify({"error": "Error interno del servidor al obtener estado de la encuesta."}), 500
    finally:
        if cur: cur.close()
        if conn: cur.close()

@app.route('/api/dashboard/form_progress', methods=['GET'])
def get_form_progress():
    """
    Endpoint to get the form completion progress for each employee.
    Calculates a completion percentage based on the presence of data in sections.
    """
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                e.id_empleado,
                e.cedula,
                e.nombres,
                e.apellidos,
                e.correo_electronico,
                e.correo_verificado,
                e.gerencia_general,
                e.gerencia_especifica,
                e.cargo,
                e.esta_estudiando_actualmente,
                e.carrera_actual,
                e.ano_actual,
                e.turno_estudio,
                d.id_estado,
                d.id_municipio,
                d.id_parroquia,
                d.id_ciudad,
                d.direccion_detallada,
                d.condicion_habitacion, -- Moved to direcciones
                e.nacionalidad, e.lugar_nacimiento, e.fecha_nacimiento, e.edad, e.estado_civil, e.sexo,
                e.mano_dominante, e.num_hijos, e.hijos_edades, e.telefono_habitacion, e.telefono_personal,
                e.telefonos_emergencia, e.profesion, e.impedimento_medico_fisico, e.talla_camisa,
                e.talla_pantalon, e.talla_calzado
            FROM empleados e
            LEFT JOIN direcciones d ON e.id_empleado = d.id_empleado;
            """
        )
        employees_raw = cur.fetchall()
        columnas = [desc[0] for desc in cur.description]
        
        progress_list = []
        for emp_data in employees_raw:
            emp_dict = dict(zip(columnas, emp_data))
            
            progress_score = 0
            
            # Categories and their max points
            # Personal Info (name, cedula, basic contact, birth info, etc.) - approx 30 points
            if all([emp_dict.get('nacionalidad'), emp_dict.get('lugar_nacimiento'), emp_dict.get('fecha_nacimiento'),
                    emp_dict.get('edad'), emp_dict.get('estado_civil'), emp_dict.get('sexo'),
                    emp_dict.get('mano_dominante'), emp_dict.get('telefono_personal'), emp_dict.get('profesion')]):
                progress_score += 15
            
            if emp_dict.get('correo_electronico') and emp_dict.get('correo_verificado'):
                progress_score += 5 # Email verification is important
            
            # Emergency contacts and children ages (flexible, add some points if present)
            if emp_dict.get('telefonos_emergencia') and emp_dict['telefonos_emergencia'] and len(json.loads(emp_dict['telefonos_emergencia'])) > 0:
                progress_score += 5
            if emp_dict.get('num_hijos') is not None and emp_dict['num_hijos'] >= 0:
                if emp_dict['num_hijos'] > 0 and emp_dict.get('hijos_edades') and emp_dict['hijos_edades'] and len(json.loads(emp_dict['hijos_edades'])) == emp_dict['num_hijos']:
                    progress_score += 5 # Additional points if ages are provided for children
                elif emp_dict['num_hijos'] == 0:
                    progress_score += 5 # Also complete if no children are declared

            # Organizational Info - 10 points
            if all([emp_dict.get('gerencia_general'), emp_dict.get('gerencia_especifica'), emp_dict.get('cargo')]):
                progress_score += 10
            
            # Address Info - 15 points
            if all([emp_dict.get('id_estado'), emp_dict.get('id_municipio'), emp_dict.get('id_parroquia'),
                    emp_dict.get('id_ciudad'), emp_dict.get('direccion_detallada'), emp_dict.get('condicion_habitacion')]):
                progress_score += 15
            
            # Uniform Measures (talla_camisa, talla_pantalon, talla_calzado) - 5 points
            if all([emp_dict.get('talla_camisa'), emp_dict.get('talla_pantalon'), emp_dict.get('talla_calzado')]):
                progress_score += 5

            # Current Study Info - 10 points
            if emp_dict.get('esta_estudiando_actualmente'):
                if all([emp_dict.get('carrera_actual'), emp_dict.get('ano_actual'), emp_dict.get('turno_estudio')]):
                    progress_score += 10
            else: # If not studying, this section is considered complete
                progress_score += 10

            # Dynamic Sections (presence of at least one entry) - Each 5 points, total 25
            cur.execute("SELECT COUNT(*) FROM educacion WHERE id_empleado = %s;", (emp_dict['id_empleado'],))
            if cur.fetchone()[0] > 0: progress_score += 5
            
            cur.execute("SELECT COUNT(*) FROM experiencia_laboral WHERE id_empleado = %s;", (emp_dict['id_empleado'],))
            if cur.fetchone()[0] > 0: progress_score += 5
            
            cur.execute("SELECT COUNT(*) FROM cursos WHERE id_empleado = %s;", (emp_dict['id_empleado'],))
            if cur.fetchone()[0] > 0: progress_score += 5

            cur.execute("SELECT COUNT(*) FROM idiomas WHERE id_empleado = %s;", (emp_dict['id_empleado'],))
            if cur.fetchone()[0] > 0: progress_score += 5

            cur.execute("SELECT COUNT(*) FROM habilidades WHERE id_empleado = %s;", (emp_dict['id_empleado'],))
            if cur.fetchone()[0] > 0: progress_score += 5
            
            percentage = min(int(progress_score), 100) # Cap at 100%
            
            progress_list.append({
                "cedula": emp_dict['cedula'],
                "nombres": emp_dict['nombres'],
                "apellidos": emp_dict['apellidos'],
                "gerencia_general": emp_dict['gerencia_general'],
                "cargo": emp_dict['cargo'],
                "porcentaje_progreso": percentage
            })
            
        return jsonify(progress_list), 200
    except Exception as e:
        app.logger.error(f"Error al obtener el progreso del formulario: {e}")
        return jsonify({"error": "Error interno del servidor al obtener el progreso del formulario."}), 500
    finally:
        if cur: cur.close()
        if conn: cur.close()

# Health check endpoint
@app.route('/health')
def health_check():
    """
    Endpoint to check the backend status and database connection.
    """
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.fetchone()
        return jsonify({"status": "ok", "message": "Backend y conexión a la base de datos exitosa."}), 200
    except Exception as e:
        app.logger.error(f"Conexión a la base de datos fallida: {e}")
        return jsonify({"status": "error", "message": f"Conexión a la base de datos fallida: {e}"}), 500
    finally:
        if cur: cur.close()
        if conn: cur.close()

# --- NEW: Data Export Endpoints ---

def authorize_hr_admin(request_data):
    """Helper function to authorize HR or Admin users."""
    cedula_autenticada = request_data.get('cedula')
    privilegio_autenticado = request_data.get('privilegio')

    if not cedula_autenticada or not privilegio_autenticado:
        return False, "Credenciales de autenticación incompletas."

    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("SELECT privilegio FROM usuarios WHERE usuario = %s;", (cedula_autenticada,))
        db_privilegio = cur.fetchone()

        if not db_privilegio or db_privilegio[0] != privilegio_autenticado:
            return False, "Privilegio de usuario no coincide o no autorizado."

        if db_privilegio[0] not in ['Admin', 'Recursos Humanos']:
            return False, "Acceso denegado. Privilegio insuficiente."
        
        return True, None
    except Exception as e:
        app.logger.error(f"Error de autorización: {e}")
        return False, "Error interno de autorización."
    finally:
        if cur: cur.close()
        if conn: cur.close()

@app.route('/api/dashboard/gender_distribution', methods=['GET'])
def get_gender_distribution():
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT sexo, COUNT(*) as count
            FROM empleados
            GROUP BY sexo
            ORDER BY sexo;
        """)
        data = [{"sexo": row[0], "count": row[1]} for row in cur.fetchall()]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al obtener distribución por género: {e}")
        return jsonify({"error": "Error al obtener datos de género."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/dashboard/marital_status', methods=['GET'])
def get_marital_status_distribution():
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT estado_civil, COUNT(*) as count
            FROM empleados
            GROUP BY estado_civil
            ORDER BY estado_civil;
        """)
        data = [{"estado_civil": row[0], "count": row[1]} for row in cur.fetchall()]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al obtener distribución por estado civil: {e}")
        return jsonify({"error": "Error al obtener datos de estado civil."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/dashboard/nationality_distribution', methods=['GET'])
def get_nationality_distribution():
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT nacionalidad, COUNT(*) as count
            FROM empleados
            GROUP BY nacionalidad
            ORDER BY nacionalidad;
        """)
        data = [{"nacionalidad": row[0], "count": row[1]} for row in cur.fetchall()]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al obtener distribución por nacionalidad: {e}")
        return jsonify({"error": "Error al obtener datos de nacionalidad."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/dashboard/experience_overview', methods=['GET'])
def get_experience_overview():
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT cargo, COUNT(*) as count
            FROM experiencia_laboral
            GROUP BY cargo
            ORDER BY count DESC
            LIMIT 10;
        """)
        data = [{"cargo": row[0], "count": row[1]} for row in cur.fetchall()]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al obtener visión general de experiencia laboral: {e}")
        return jsonify({"error": "Error al obtener datos de experiencia laboral."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/dashboard/languages_by_level', methods=['GET'])
def get_languages_by_level():
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT idioma, nivel, COUNT(*) as count
            FROM idiomas
            GROUP BY idioma, nivel
            ORDER BY idioma, nivel;
        """)
        data = [{"idioma": row[0], "nivel": row[1], "count": row[2]} for row in cur.fetchall()]
        return jsonify(data), 200
    except Exception as e:
        app.logger.error(f"Error al obtener idiomas por nivel: {e}")
        return jsonify({"error": "Error al obtener datos de idiomas."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/email/enviar_codigo', methods=['POST'])
def enviar_codigo_email():
    """
    Endpoint para enviar un código de verificación por correo electrónico.
    Acepta JSON: { "email": "...", "correo": "...", "cedula": "..." }
    Genera el código automáticamente si no se envía.
    """
    data = request.get_json()
    correo = data.get('correo') or data.get('email')
    cedula = data.get('cedula')
    codigo = data.get('codigo') or str(secrets.randbelow(900000) + 100000)  # Genera un código de 6 dígitos si no viene

    if not correo or not cedula:
        return jsonify({"error": "Correo y cédula son requeridos."}), 400

    # Opcional: Guarda el código en la base de datos para validación posterior
    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute(
            "UPDATE empleados SET codigo_verificacion = %s, codigo_expira_en = %s WHERE cedula = %s;",
            (codigo, datetime.now() + timedelta(minutes=10), cedula)
        )
        conn.commit()
    except Exception as e:
        app.logger.error(f"Error al guardar código de verificación: {e}")
        if conn: conn.rollback()
    finally:
        if cur: cur.close()
        if conn: conn.close()

    try:

        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        msg = MIMEMultipart("alternative")
        msg['Subject'] = "Código de verificación de correo"
        msg['From'] = EMAIL_SENDER
        msg['To'] = correo

        html = f"""
        <html>
        <body style="font-family: Arial, sans-serif; background-color: #f9fafb; color: #1f2937; padding: 40px 20px; text-align: center;">
            <div style="max-width: 600px; margin: auto; background-color: #ffffff; border-radius: 10px; padding: 30px; box-shadow: 0 2px 6px rgba(0,0,0,0.1);">
            <h2 style="color: #2563eb; margin-bottom: 10px;">Verificación de Correo Electrónico</h2>
            <p>Hola,</p>
            <p>Para proteger tu identidad y garantizar la seguridad de tu información, por favor utiliza el siguiente código de verificación:</p>
            <div style="margin: 32px 0;">
                <span style="font-size: 2.5rem; background-color: #f1f5f9; color: #2563eb; font-weight: bold; padding: 16px 32px; border: 2px solid #2563eb; border-radius: 10px; display: inline-block; letter-spacing: 6px;">
                {codigo}
                </span>
            </div>
            <p>Ingresa este código en el formulario correspondiente para completar la verificación.</p>
            <p style="color: #ef4444; font-weight: bold;">⚠ Este código es personal y expirará en 1 minuto. No lo compartas con nadie.</p>
            <hr style="margin: 30px 0; border: none; border-top: 1px solid #e5e7eb;">
            <p style="font-size: 0.95em;">Este mensaje ha sido emitido por la <strong>Gerencia General de Gestión Humana</strong>.</p>
            <p style="font-size: 0.9em; color: #6b7280;">📧 GestionHuanaCiip@gmail.com | ☎ (212) 274-3742</p>
            </div>
        </body>
        </html>
        """

        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, [correo], msg.as_string())

        return jsonify({"mensaje": "Código de verificación enviado. Revise su bandeja de entrada."}), 200
    except Exception as e:
        app.logger.error(f"Error al enviar correo: {e}")
        return jsonify({"error": "Error al enviar el correo."}), 500

@app.route('/api/email/verificar_codigo', methods=['POST'])
def verificar_codigo_email():
    """
    Verifica únicamente el código de verificación enviado al correo para la cédula.
    Espera JSON: { "correo": "...", "cedula": "...", "codigo": "123456" }
    """
    data = request.get_json()
    cedula = data.get('cedula')
    codigo = data.get('codigo') or data.get('code')


    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute(
            "SELECT codigo_verificacion FROM empleados WHERE cedula = %s;",
            (cedula,)
        )
        result = cur.fetchone()
        if not result:
            return jsonify({"error": "Empleado no encontrado o correo no coincide."}), 404

        codigo_db = result[0]
        if not codigo_db:
            return jsonify({"error": "No hay código de verificación registrado."}), 400

        if str(codigo_db) != str(codigo):
            return jsonify({"error": "El código es incorrecto."}), 400

        return jsonify({"mensaje": "Código verificado correctamente."}), 200

    except Exception as e:
        app.logger.error(f"Error al verificar código de correo: {e}")
        return jsonify({"error": "Error al verificar el código."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/data/export/<string:table_name>', methods=['POST'])
def export_table_data(table_name):
    """
    Exports data from a specified table as CSV.
    Requires HR or Admin privilege.
    """
    auth_data = request.get_json()
    authorized, error_message = authorize_hr_admin(auth_data)
    if not authorized:
        return jsonify({"error": error_message}), 403

    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        # Sanitize table_name to prevent SQL Injection
        if not table_name.replace('_', '').isalnum():
            return jsonify({"error": "Nombre de tabla inválido."}), 400
        
        # Mapping table names to their actual SQL table names and specific columns
        table_map = {
            'empleados': 'empleados',
            'direcciones': 'direcciones',
            'educacion': 'educacion',
            'experiencia_laboral': 'experiencia_laboral',
            'cursos': 'cursos',
            'idiomas': 'idiomas',
            'habilidades': 'habilidades',
            'usuarios': 'usuarios',
            'realizados': 'realizados',
            'ubicaciones_localidad': 'ubicaciones_localidad',
            'ubicaciones_inmueble': 'ubicaciones_inmueble',
            'solicitudes_asistencia': 'solicitudes_asistencia',
            'ubicaciones_estados': 'ubicaciones_estados',
            'ubicaciones_municipios': 'ubicaciones_municipios',
            'ubicaciones_parroquias': 'ubicaciones_parroquias',
            'ubicaciones_ciudades': 'ubicaciones_ciudades',
            'precarga_personal': 'precarga_personal',
            'cargos': 'cargos','gerencias_generales': 'gerencias_generales',
            'gerencias_especificas': 'gerencias_especificas',
        }
        
        db_table_name = table_map.get(table_name)
        if not db_table_name:
            return jsonify({"error": "Tabla no encontrada para exportar."}), 404

        cur.execute(f"SELECT * FROM {db_table_name};")
        rows = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]

        data = []
        for row in rows:
            row_dict = {}
            for i, col_name in enumerate(column_names):
                value = row[i]
                # Convert datetime objects to string format for CSV
                if isinstance(value, datetime):
                    row_dict[col_name] = value.isoformat()
                # Handle JSONB fields
                elif isinstance(value, (dict, list)): # Directly check if it's a dict or list
                    row_dict[col_name] = json.dumps(value)
                else:
                    row_dict[col_name] = value
            data.append(row_dict)

        # --- NUEVO BLOQUE: Si no hay datos, igual retorna los encabezados ---
        if not data:
            # Retorna solo los encabezados como una fila vacía (para CSV)
            return jsonify({
                "headers": column_names,
                "rows": []
            }), 200
        # --- FIN NUEVO BLOQUE ---

        return jsonify(data), 200

    except psycopg2.Error as e:
        app.logger.error(f"Error de base de datos al exportar {table_name}: {e}")
        return jsonify({"error": f"Error al exportar datos de {table_name}."}), 500
    except Exception as e:
        app.logger.error(f"Error inesperado al exportar {table_name}: {e}")
        return jsonify({"error": f"Error inesperado del servidor al exportar {table_name}."}), 500
    finally:
        if cur: cur.close()
        if conn: cur.close()

@app.route('/api/data/export_all', methods=['POST'])
def export_all_tables():
    """
    Exports all relevant tables as a ZIP file containing CSVs.
    Requires HR or Admin privilege.
    """
    auth_data = request.get_json()
    authorized, error_message = authorize_hr_admin(auth_data)
    if not authorized:
        return jsonify({"error": error_message}), 403

    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        tables_to_export = [
            'empleados', 'direcciones', 'educacion', 'experiencia_laboral', 'cursos',
            'idiomas', 'habilidades', 'usuarios', 'realizados', 'solicitudes_asistencia',
            'ubicaciones_localidad', 'ubicaciones_inmueble', 'ubicaciones_estados', 
            'ubicaciones_municipios', 'ubicaciones_parroquias', 'ubicaciones_ciudades',
            'precarga_personal','gerencias_generales', 'gerencias_especificas', 'cargos', # Added new table
        ]

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for table_name in tables_to_export:
                cur.execute(f"SELECT * FROM {table_name};")
                rows = cur.fetchall()
                column_names = [desc[0] for desc in cur.description]

                csv_buffer = io.StringIO()
                # For export, use semicolon for headers and comma for data for consistency with import request
                csv_writer = csv.writer(csv_buffer, delimiter=',') 
                
                # Write headers with semicolon (if desired, but usually export should match import for consistency)
                # For this specific request, the headers are separated by semicolons
                csv_buffer.write(';'.join(column_names) + '\n')

                for row in rows:
                    processed_row = []
                    for i, col_name in enumerate(column_names):
                        value = row[i]
                        if isinstance(value, datetime):
                            processed_row.append(value.isoformat())
                        elif isinstance(value, (dict, list)): # Handle JSONB fields
                            processed_row.append(json.dumps(value))
                        else:
                            processed_row.append(value)
                    csv_writer.writerow(processed_row)
                
                csv_buffer.seek(0)
                zip_file.writestr(f"{table_name}.csv", csv_buffer.getvalue())
        
        zip_buffer.seek(0)
        return send_file(zip_buffer, mimetype='application/zip', as_attachment=True, download_name='all_database_tables.zip'), 200

    except psycopg2.Error as e:
        app.logger.error(f"Error de base de datos al exportar todas las tablas: {e}")
        return jsonify({"error": "Error al exportar todas las tablas."}), 500
    except Exception as e:
        app.logger.error(f"Error inesperado al exportar todas las tablas: {e}")
        return jsonify({"error": "Error inesperado del servidor al exportar todas las tablas."}), 500
    finally:
        if cur: cur.close()
        if conn: cur.close()

# --- NEW: Data Import Endpoints ---

@app.route('/api/data/import/<string:table_name>', methods=['POST'])
def import_table_data(table_name):
    """
    Imports data into a specified table from CSV.
    Headers are expected to be semicolon-separated.
    Data rows are expected to be comma-separated.
    Requires HR or Admin privilege.
    """
    # Authorization using headers for CSV upload
    cedula_autenticada = request.headers.get('X-User-Cedula')
    privilegio_autenticado = request.headers.get('X-User-Privilegio')
    authorized, error_message = authorize_hr_admin({'cedula': cedula_autenticada, 'privilegio': privilegio_autenticado})
    if not authorized:
        return jsonify({"error": error_message}), 403

    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        conn.autocommit = False # Start transaction
        cur = conn.cursor()

        if not table_name.replace('_', '').isalnum():
            conn.rollback()
            return jsonify({"error": "Nombre de tabla inválido."}), 400

        # Mapping table names for import, consider potential foreign key relationships
        table_map = {
            'empleados': 'empleados',
            'direcciones': 'direcciones',
            'educacion': 'educacion',
            'experiencia_laboral': 'experiencia_laboral',
            'cursos': 'cursos',
            'idiomas': 'idiomas',
            'habilidades': 'habilidades',
            'usuarios': 'usuarios',
            'realizados': 'realizados',
            'solicitudes_asistencia': 'solicitudes_asistencia',
            'precarga_personal': 'precarga_personal',
            'ubicaciones_localidad': 'ubicaciones_localidad',
            'ubicaciones_inmueble': 'ubicaciones_inmueble',
            'ubicaciones_estados': 'ubicaciones_estados',
            'ubicaciones_municipios': 'ubicaciones_municipios',
            'ubicaciones_parroquias': 'ubicaciones_parroquias',
            'ubicaciones_ciudades': 'ubicaciones_ciudades'
        }
        
        db_table_name = table_map.get(table_name)
        if not db_table_name:
            conn.rollback()
            return jsonify({"error": "Importación no soportada para esta tabla."}), 400

        csv_data = request.data.decode('utf-8-sig')
        lines = csv_data.strip().split('\n')
        
        if not lines:
            return jsonify({"error": "Archivo CSV vacío."}), 400

        # Read headers using semicolon delimiter
        header_line = lines[0]
        csv_headers_raw = [h.strip().strip('"') for h in header_line.split(';')]
        csv_headers_normalized = [h.lower() for h in csv_headers_raw]
        app.logger.info(f"Normalized CSV headers read for {table_name} (semicolon delimited): {csv_headers_normalized}")

        # Get actual table columns and their data types from DB to handle type conversions
        cur.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = %s
            ORDER BY ordinal_position;
        """, (db_table_name,))
        db_columns_info = cur.fetchall() # [(col_name, data_type), ...]
        
        db_columns_raw = [info[0] for info in db_columns_info]
        db_columns_normalized_map = {c.lower(): c for c in db_columns_raw} # {normalized_name: raw_name} for lookup
        
        # Map database types to Python types for _process_value
        column_type_map = {}
        for col_name, data_type in db_columns_info:
            normalized_col_name = col_name.lower()
            if 'char' in data_type or 'text' in data_type:
                column_type_map[normalized_col_name] = str
            elif 'int' in data_type:
                column_type_map[normalized_col_name] = int
            elif 'numeric' in data_type or 'float' in data_type or 'double' in data_type:
                column_type_map[normalized_col_name] = float
            elif 'bool' in data_type:
                column_type_map[normalized_col_name] = bool
            elif 'date' in data_type:
                column_type_map[normalized_col_name] = datetime.date
            elif 'timestamp' in data_type:
                column_type_map[normalized_col_name] = datetime
            elif data_type == 'jsonb':
                column_type_map[normalized_col_name] = dict # Could be list too, but dict is a safe default
            else:
                column_type_map[normalized_col_name] = str # Default to string
        app.logger.info(f"Column type map for {table_name}: {column_type_map}")

        # Basic header validation: ensure all CSV headers exist in DB columns (case-insensitive)
        missing_headers_in_db = [
            h_raw for h_raw, h_norm in zip(csv_headers_raw, csv_headers_normalized)
            if h_norm not in db_columns_normalized_map
        ]
        if missing_headers_in_db:
            app.logger.error(f"Import error for table {table_name}: CSV headers missing in DB columns: {missing_headers_in_db}")
            conn.rollback()
            return jsonify({"error": f"Encabezados CSV no coinciden con las columnas de la tabla. Faltan: {', '.join(missing_headers_in_db)}. Verifique la plantilla."}), 400

        rows_inserted = 0
        rows_updated = 0
        
        # Process data rows using comma delimiter
        data_lines = lines[1:]
        
        # For tables with unique constraints or specific upsert logic
        if db_table_name == 'empleados':
            for line in data_lines:
                if not line.strip(): continue # Skip empty lines
                # Use csv.reader for comma-separated data rows
                row_data = next(csv.reader(io.StringIO(line), delimiter=','))
                
                # Create a dict from normalized headers and raw row data
                row_dict_raw = dict(zip(csv_headers_normalized, row_data))
                
                # Process values using the type map
                row_dict = {
                    key: _process_value(val, target_type=column_type_map.get(key, str))
                    for key, val in row_dict_raw.items()
                }

                cedula = row_dict.get('cedula')
                if not cedula:
                    app.logger.warning(f"Skipping row in 'empleados' import: 'cedula' field is missing or empty. Row data: {row_dict_raw}")
                    continue
                cedula = cedula.upper()

                cur.execute("SELECT id_empleado FROM empleados WHERE cedula = %s;", (cedula,))
                existing_employee = cur.fetchone()

                # Dynamically build columns and values for insert/update based on what's available and processed
                cols_to_use = []
                values_to_use = []
                
                for db_col_raw in db_columns_raw:
                    normalized_db_col = db_col_raw.lower()
                    
                    if db_col_raw == 'id_empleado': continue # Skip primary key 'id_empleado' for inserts/updates

                    # Check if the CSV provided a value for this DB column
                    if normalized_db_col in row_dict:
                        val = row_dict[normalized_db_col]
                        # Only include if value is not None (empty in CSV)
                        if val is not None:
                            cols_to_use.append(db_col_raw)
                            values_to_use.append(val)
                    elif db_col_raw == 'correo_verificado' and not existing_employee:
                        # For new inserts, if 'correo_verificado' not provided, default to TRUE
                        cols_to_use.append(db_col_raw)
                        values_to_use.append(True)
                    # For other auto-generated timestamps, let DB defaults handle them implicitly

                if existing_employee:
                    # If no fields are provided in CSV (i.e. cols_to_use is empty), skip update
                    if not cols_to_use:
                        app.logger.info(f"Skipping update for employee {cedula}: no valid fields provided in CSV.")
                        continue

                    # Construct UPDATE statement
                    update_set_clauses = [f"{col} = %s" for col in cols_to_use]
                    update_query = f"""
                        UPDATE {db_table_name}
                        SET {', '.join(update_set_clauses)}, actualizado_en = CURRENT_TIMESTAMP
                        WHERE cedula = %s;
                    """
                    cur.execute(update_query, (*values_to_use, cedula))
                    rows_updated += cur.rowcount
                else:
                    # Ensure 'cedula' is always part of the insert. If it wasn't in cols_to_use from dynamic check.
                    if 'cedula' not in [c.lower() for c in cols_to_use]: # Check case-insensitively
                        cols_to_use.append('cedula')
                        values_to_use.append(cedula)
                        
                    insert_columns_str = ', '.join(cols_to_use)
                    placeholders = ', '.join(['%s'] * len(values_to_use))
                    insert_query = f"""
                        INSERT INTO {db_table_name} ({insert_columns_str})
                        VALUES ({placeholders}) RETURNING id_empleado;
                    """
                    cur.execute(insert_query, tuple(values_to_use))
                    rows_inserted += cur.rowcount # For new inserts, id_empleado is returned but not used here directly

        elif db_table_name == 'direcciones':
            for line in data_lines:
                if not line.strip(): continue
                row_data = next(csv.reader(io.StringIO(line), delimiter=','))
                row_dict_raw = dict(zip(csv_headers_normalized, row_data))
                row_dict = {
                    key: _process_value(val, target_type=column_type_map.get(key, str))
                    for key, val in row_dict_raw.items()
                }

                cedula_empleado = row_dict.get('cedula_empleado')
                if not cedula_empleado:
                    app.logger.warning(f"Skipping row in 'direcciones' import: 'cedula_empleado' field is missing or empty. Row data: {row_dict_raw}")
                    continue
                cedula_empleado = cedula_empleado.upper()

                cur.execute("SELECT id_empleado FROM empleados WHERE cedula = %s;", (cedula_empleado,))
                empleado_id_result = cur.fetchone()
                if not empleado_id_result:
                    app.logger.warning(f"Employee with cedula {cedula_empleado} not found for address import. Skipping row.")
                    continue
                id_empleado = empleado_id_result[0]

                # Fetch IDs for location names (assuming headers are 'estado', 'municipio', etc.)
                id_estado = None
                if row_dict.get('estado'):
                    cur.execute("SELECT id_estado FROM ubicaciones_estados WHERE nombre = %s;", (row_dict['estado'],))
                    res = cur.fetchone()
                    if res: id_estado = res[0]

                id_municipio = None
                if row_dict.get('municipio'):
                    cur.execute("SELECT id_municipio FROM ubicaciones_municipios WHERE nombre = %s;", (row_dict['municipio'],))
                    res = cur.fetchone()
                    if res: id_municipio = res[0]
                
                id_parroquia = None
                if row_dict.get('parroquia'):
                    cur.execute("SELECT id_parroquia FROM ubicaciones_parroquias WHERE nombre = %s;", (row_dict['parroquia'],))
                    res = cur.fetchone()
                    if res: id_parroquia = res[0]

                id_ciudad = None
                if row_dict.get('ciudad'):
                    cur.execute("SELECT id_ciudad FROM ubicaciones_ciudades WHERE nombre = %s;", (row_dict['ciudad'],))
                    res = cur.fetchone()
                    if res: id_ciudad = res[0]

                cur.execute("SELECT id_direccion FROM direcciones WHERE id_empleado = %s;", (id_empleado,))
                existing_direccion = cur.fetchone()
                
                # Dynamically build columns and values for insert/update
                cols_to_use = []
                values_to_use = []

                # Explicitly map location names to IDs, and other address fields
                location_fields = {
                    'id_estado': id_estado,
                    'id_municipio': id_municipio,
                    'id_parroquia': id_parroquia,
                    'id_ciudad': id_ciudad,
                    'direccion_detallada': row_dict.get('direccion_detallada'),
                    'condicion_habitacion': row_dict.get('condicion_habitacion')
                }

                for col_name_in_db, value in location_fields.items():
                    if value is not None:
                        cols_to_use.append(col_name_in_db)
                        values_to_use.append(value)

                if existing_direccion:
                    if not cols_to_use:
                        app.logger.info(f"Skipping update for address of employee {cedula_empleado}: no valid fields provided in CSV.")
                        continue
                    update_set_clauses = [f"{col} = %s" for col in cols_to_use]
                    update_query = f"""
                        UPDATE {db_table_name}
                        SET {', '.join(update_set_clauses)}, actualizado_en = CURRENT_TIMESTAMP
                        WHERE id_empleado = %s;
                    """
                    cur.execute(update_query, (*values_to_use, id_empleado))
                    rows_updated += cur.rowcount
                else:
                    if not cols_to_use: # If no address data at all, don't insert a blank row
                        app.logger.info(f"Skipping insert for address of employee {cedula_empleado}: no valid fields provided in CSV.")
                        continue
                    insert_columns_str = ', '.join(cols_to_use + ['id_empleado'])
                    placeholders = ', '.join(['%s'] * (len(cols_to_use) + 1))
                    insert_query = f"""
                        INSERT INTO {db_table_name} ({insert_columns_str})
                        VALUES ({placeholders});
                    """
                    cur.execute(insert_query, (*values_to_use, id_empleado))
                    rows_inserted += cur.rowcount

        elif db_table_name in ['educacion', 'experiencia_laboral', 'cursos', 'idiomas', 'habilidades']:
            # For these related tables, first delete existing for the employee, then insert new
            # Read all rows into memory first to handle potential out-of-order data or multiple entries for one employee
            all_rows_data_for_table = []
            for line in data_lines:
                if not line.strip(): continue
                row_data = next(csv.reader(io.StringIO(line), delimiter=','))
                row_dict_raw = dict(zip(csv_headers_normalized, row_data))
                all_rows_data_for_table.append(row_dict_raw)

            processed_employees = set() # To track which employees we've processed deletes for
            
            for row_dict_raw in all_rows_data_for_table:
                # Process values using the type map
                row_dict = {
                    key: _process_value(val, target_type=column_type_map.get(key, str))
                    for key, val in row_dict_raw.items()
                }

                cedula_empleado = row_dict.get('cedula_empleado')
                if not cedula_empleado:
                    app.logger.warning(f"Skipping row in import of {table_name}: 'cedula_empleado' field is missing or empty. Row data: {row_dict_raw}")
                    continue
                cedula_empleado = cedula_empleado.upper()

                cur.execute("SELECT id_empleado FROM empleados WHERE cedula = %s;", (cedula_empleado,))
                empleado_id_result = cur.fetchone()
                if not empleado_id_result:
                    app.logger.warning(f"Employee with cedula {cedula_empleado} not found for import into {table_name}. Skipping row.")
                    continue
                id_empleado = empleado_id_result[0]

                # Delete existing records for this employee for the current table ONLY ONCE
                if id_empleado not in processed_employees:
                    cur.execute(f"DELETE FROM {db_table_name} WHERE id_empleado = %s;", (id_empleado,))
                    processed_employees.add(id_empleado)

                # Dynamically build columns and values for insert
                cols_to_use = ['id_empleado']
                values_to_use = [id_empleado]

                for db_col_raw in db_columns_raw:
                    normalized_db_col = db_col_raw.lower()
                    # Skip 'id_empleado' as it's handled above
                    if db_col_raw == 'id_empleado': continue 

                    # Only include if the value is explicitly provided in CSV and not None
                    if normalized_db_col in row_dict and row_dict[normalized_db_col] is not None:
                        cols_to_use.append(db_col_raw)
                        values_to_use.append(row_dict[normalized_db_col])
                
                # Ensure at least one actual data column is present besides id_empleado
                if len(cols_to_use) <= 1:
                    app.logger.warning(f"Skipping empty row for employee {cedula_empleado} in {table_name}. No valid data fields found.")
                    continue

                insert_columns_str = ', '.join(cols_to_use)
                placeholders = ', '.join(['%s'] * len(values_to_use))
                insert_query = f"""
                    INSERT INTO {db_table_name} ({insert_columns_str})
                    VALUES ({placeholders});
                """
                cur.execute(insert_query, tuple(values_to_use))
                rows_inserted += cur.rowcount

        elif db_table_name == 'precarga_personal': # NEW: Specific logic for precarga_personal import (upsert)
            for line in data_lines:
                if not line.strip(): continue
                row_data = next(csv.reader(io.StringIO(line), delimiter=','))
                row_dict_raw = dict(zip(csv_headers_normalized, row_data))
                
                row_dict = {
                    key: _process_value(val, target_type=column_type_map.get(key, str))
                    for key, val in row_dict_raw.items()
                }

                cedula = row_dict.get('cedula')
                nombres = row_dict.get('nombres')
                apellidos = row_dict.get('apellidos')

                if not all([cedula, nombres, apellidos]):
                    app.logger.warning(f"Skipping row in 'precarga_personal' import: Missing cedula, nombres, or apellidos. Row data: {row_dict_raw}")
                    continue
                cedula = cedula.upper()

                cur.execute(
                    """
                    INSERT INTO precarga_personal (cedula, nombres, apellidos)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (cedula) DO UPDATE
                    SET nombres = EXCLUDED.nombres, apellidos = EXCLUDED.apellidos, actualizado_en = CURRENT_TIMESTAMP;
                    """,
                    (cedula, nombres, apellidos)
                )
                rows_inserted += cur.rowcount # This counts as an insert or an update row affected


        else: # Generic insert for other tables (e.g., users, realizados, solicitudes_asistencia)
            for line in data_lines:
                if not line.strip(): continue
                row_data = next(csv.reader(io.StringIO(line), delimiter=','))
                row_dict_raw = dict(zip(csv_headers_normalized, row_data))
                
                # Process values using the type map
                row_dict = {
                    key: _process_value(val, target_type=column_type_map.get(key, str))
                    for key, val in row_dict_raw.items()
                }
                
                # Handle specific table logic like password hashing for 'usuarios'
                if db_table_name == 'usuarios' and 'clave' in row_dict and row_dict['clave'] is not None:
                    row_dict['clave'] = generate_password_hash(row_dict['clave'])
                
                # Dynamically build columns and values for insert
                cols_to_use = []
                values_to_use = []
                for db_col_raw in db_columns_raw:
                    normalized_db_col = db_col_raw.lower()
                    # Skip primary key if it's auto-generated and not provided
                    if db_col_raw.startswith('id_') and normalized_db_col not in row_dict:
                        continue # Let DB handle auto-incrementing IDs

                    if normalized_db_col in row_dict and row_dict[normalized_db_col] is not None:
                        cols_to_use.append(db_col_raw)
                        values_to_use.append(row_dict[normalized_db_col])
                
                if not cols_to_use:
                    app.logger.warning(f"Skipping empty row in {table_name}. No valid data fields found.")
                    continue

                insert_columns_str = ', '.join(cols_to_use)
                placeholders = ', '.join(['%s'] * len(values_to_use))
                insert_query = f"""
                    INSERT INTO {db_table_name} ({insert_columns_str})
                    VALUES ({placeholders});
                """
                cur.execute(insert_query, tuple(values_to_use))
                rows_inserted += cur.rowcount


        conn.commit()
        return jsonify({"message": f"Importación a '{table_name}' completada. Filas insertadas: {rows_inserted}, Filas actualizadas: {rows_updated}."}), 200

    except psycopg2.Error as e:
        if conn: conn.rollback()
        app.logger.error(f"Error de base de datos durante la importación a {table_name}: {e}")
        return jsonify({"error": f"Error al importar datos a {table_name}: {e}"}), 500
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error inesperado durante la importación a {table_name}: {e}")
        return jsonify({"error": f"Error inesperado del servidor al importar a {table_name}: {e}"}), 500
    finally:
        if cur: cur.close()
        if conn: cur.close()

# --- NEW: Endpoint for Pre-load Personnel ---
@app.route('/api/rrhh/precarga_personal', methods=['POST'])
def precarga_personal_endpoint():
    """
    Endpoint for HR to pre-load basic personnel information (cedula, nombres, apellidos, gerencia_general, gerencia_especifica, cargo).
    Performs an upsert operation.
    Requires HR or Admin privilege.
    """
    auth_data = request.get_json()
    cedula_autenticada = auth_data.get('X-User-Cedula') or request.headers.get('X-User-Cedula')
    privilegio_autenticado = auth_data.get('X-User-Privilegio') or request.headers.get('X-User-Privilegio')

    authorized, error_message = authorize_hr_admin({'cedula': cedula_autenticada, 'privilegio': privilegio_autenticado})
    if not authorized:
        return jsonify({"error": error_message}), 403

    data = request.get_json()
    cedula = data.get('cedula').upper() if data.get('cedula') else None
    nombres = data.get('nombres')
    apellidos = data.get('apellidos')
    gerencia_general = data.get('gerencia_general')
    gerencia_especifica = data.get('gerencia_especifica')
    cargo = data.get('cargo')

    if not all([cedula, nombres, apellidos]):
        return jsonify({"error": "Cédula, nombres y apellidos son requeridos para la precarga."}), 400

    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        # Check if the employee already exists in 'empleados'
        cur.execute("SELECT cedula FROM empleados WHERE cedula = %s;", (cedula,))
        empleado_existente = cur.fetchone()

        if empleado_existente:
            conn.rollback()
            return jsonify({"error": "El empleado con esta cédula ya existe en la base de datos principal. No es necesario precargar."}), 409

        cur.execute(
            """
            INSERT INTO precarga_personal (cedula, nombres, apellidos, gerencia_general, gerencia_especifica, cargo)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (cedula) DO UPDATE
            SET nombres = EXCLUDED.nombres,
                apellidos = EXCLUDED.apellidos,
                gerencia_general = EXCLUDED.gerencia_general,
                gerencia_especifica = EXCLUDED.gerencia_especifica,
                cargo = EXCLUDED.cargo,
                actualizado_en = CURRENT_TIMESTAMP;
            """,
            (cedula, nombres, apellidos, gerencia_general, gerencia_especifica, cargo)
        )
        conn.commit()
        return jsonify({"message": "Datos de precarga guardados/actualizados con éxito."}), 200
    except psycopg2.Error as e:
        if conn: conn.rollback()
        app.logger.error(f"Error de base de datos al precargar personal: {e}")
        return jsonify({"error": f"Error al precargar personal: {e}"}), 500
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error inesperado al precargar personal: {e}")
        return jsonify({"error": f"Error inesperado del servidor al precargar personal: {e}"}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/empleados/consultar_precarga_cedula', methods=['GET'])
def consultar_precarga_por_cedula():
    """
    Consulta los datos de precarga_personal por cédula.
    """
    cedula = request.args.get('cedula')
    if not cedula:
        return jsonify({"error": "Debe proporcionar una cédula."}), 400

    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT cedula, nombres, apellidos, gerencia_general, gerencia_especifica, cargo
            FROM precarga_personal
            WHERE cedula = %s;
        """, (cedula,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Precarga no encontrada para esta cédula."}), 404

        columnas = [desc[0] for desc in cur.description]
        precarga = dict(zip(columnas, row))
        return jsonify(precarga), 200
    except Exception as e:
        app.logger.error(f"Error al consultar precarga por cédula: {e}")
        return jsonify({"error": "Error interno del servidor."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route('/api/empleados/consultar_cedula', methods=['GET'])
def consultar_empleado_por_cedula():
    """
    Consulta los datos de un empleado por su cédula, incluyendo dirección y nombres de localidad/inmueble.
    """
    cedula = request.args.get('cedula')
    if not cedula:
        return jsonify({"error": "Debe proporcionar una cédula."}), 400

    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT e.id_empleado AS id_empleado_principal, e.*, d.id_direccion, d.id_estado, d.id_municipio, d.id_parroquia, d.id_ciudad,
                d.localidad, ul.nombre AS nombre_localidad_descriptivo,
                d.nombre_localidad,
                d.tipo_inmueble, ui.nombre AS nombre_inmueble_descriptivo,
                d.numero_casa, d.edificio_bloque_torre,
                d.piso, d.puerta, d.direccion_detallada, d.zona_postal, d.condicion_habitacion
            FROM empleados e
            LEFT JOIN direcciones d ON e.id_empleado = d.id_empleado
            LEFT JOIN ubicaciones_localidad ul ON d.localidad = ul.id
            LEFT JOIN ubicaciones_inmueble ui ON d.tipo_inmueble = ui.id
            WHERE e.cedula = %s;
        """, (cedula,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Empleado no encontrado."}), 404
        
        columnas = [desc[0] for desc in cur.description]
        empleado = dict(zip(columnas, row))

        id_empleado = empleado.get('id_empleado_principal')

        # Cargar cursos, habilidades, idiomas, referencias_personales
        empleado['cursos'] = []
        empleado['habilidades'] = []
        empleado['idiomas'] = []
        empleado['referencias_personales'] = []

        cur.execute("SELECT * FROM cursos WHERE id_empleado = %s;", (id_empleado,))
        empleado['cursos'] = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

        cur.execute("SELECT * FROM habilidades WHERE id_empleado = %s;", (id_empleado,))
        empleado['habilidades'] = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

        cur.execute("SELECT * FROM idiomas WHERE id_empleado = %s;", (id_empleado,))
        empleado['idiomas'] = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

        cur.execute("SELECT * FROM referencias_personales WHERE id_empleado = %s;", (id_empleado,))
        empleado['referencias_personales'] = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

        # Teléfonos de emergencia
        empleado['telefonos_emergencia'] = []
        cur.execute("SELECT nombre, telefono, parentesco FROM Telefono_Emergencia WHERE id_personal = %s;", (empleado['id_empleado'],))
        for tel in cur.fetchall():
            empleado['telefonos_emergencia'].append({
                "nombre": tel[0],
                "telefono": tel[1],
                "parentesco": tel[2]
            })

        return jsonify(empleado), 200
    except Exception as e:
        app.logger.error(f"Error al consultar empleado por cédula: {e}")
        return jsonify({"error": "Error interno del servidor."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

# Add the following routes to the Flask app, outside of any existing functions
# Reemplaza tu función guardar_declaracion_ruta completa con esta versión
@app.route('/api/declaracion_ruta', methods=['POST'])
def guardar_declaracion_ruta():
    data = request.get_json()
    cedula = _process_value(data.get('cedula'), str)

    if not cedula:
        return jsonify({"error": "Cédula es requerida."}), 400

    # Procesar todos los campos entrantes usando _process_value para asegurar el tipo correcto
    origen = _process_value(data.get('origen'), str)
    destino = _process_value(data.get('destino'), str)
    transporte_ida = _process_value(data.get('transporte_ida'), str)
    transporte_ida_otro = _process_value(data.get('transporte_ida_otro'), str)
    hora_salida_ida = _process_value(data.get('hora_salida_ida'), str) # Se guarda como string "HH:MM"
    transporte_regreso = _process_value(data.get('transporte_regreso'), str)
    transporte_regreso_otro = _process_value(data.get('transporte_regreso_otro'), str)
    ruta_alterna_requerida = _process_value(data.get('ruta_alterna_requerida'), bool)
    ruta_alterna_descripcion = _process_value(data.get('ruta_alterna_descripcion'), str)

    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        # Obtener id_empleado
        cur.execute("SELECT id_empleado FROM empleados WHERE cedula = %s;", (cedula,))
        empleado_id_result = cur.fetchone()
        if not empleado_id_result:
            return jsonify({"error": "Empleado no encontrado."}), 404
        id_empleado = empleado_id_result[0]

        # Verificar si ya existe una declaración para este empleado para la fecha actual
        cur.execute("""
            SELECT id_declaracion FROM declaraciones_ruta
            WHERE id_empleado = %s AND fecha_declaracion = CURRENT_DATE;
        """, (id_empleado,))
        existe_declaracion = cur.fetchone()

        if existe_declaracion:
            # Actualizar la declaración existente
            cur.execute(
                """
                UPDATE declaraciones_ruta
                SET origen = %s, destino = %s, transporte_ida = %s,
                transporte_ida_otro = %s, hora_salida_ida = %s, transporte_regreso = %s,
                transporte_regreso_otro = %s, ruta_alterna_requerida = %s,
                ruta_alterna_descripcion = %s, actualizado_en = CURRENT_TIMESTAMP
                WHERE id_declaracion = %s;
                """,
                (origen, destino, transporte_ida, transporte_ida_otro,
                 hora_salida_ida,
                 transporte_regreso, transporte_regreso_otro,
                 ruta_alterna_requerida, ruta_alterna_descripcion,
                 existe_declaracion[0]) # Usamos id_declaracion
            )
        else:
            # Insertar nueva declaración
            cur.execute(
                """
                INSERT INTO declaraciones_ruta (
                    id_empleado, origen, destino, transporte_ida, transporte_ida_otro,
                    hora_salida_ida, transporte_regreso, transporte_regreso_otro, ruta_alterna_requerida,
                    ruta_alterna_descripcion
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (id_empleado, origen, destino, transporte_ida, transporte_ida_otro,
                 hora_salida_ida,
                 transporte_regreso, transporte_regreso_otro,
                 ruta_alterna_requerida, ruta_alterna_descripcion)
            )

        # Actualizar el estado de llenado en la tabla de empleados (si aplica)
        cur.execute("""
            UPDATE empleados
            SET declaracion_ruta_llena = TRUE, actualizado_en = CURRENT_TIMESTAMP
            WHERE id_empleado = %s;
        """, (id_empleado,))

        conn.commit()
        return jsonify({"message": "Declaración de ruta guardada/actualizada exitosamente."}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        app.logger.error(f"Error al guardar declaración de ruta: {e}")
        return jsonify({"error": "Error interno del servidor al guardar declaración de ruta."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

# Locate the @app.route('/api/declaracion_ruta/<string:cedula>', methods=['GET']) function
# And replace its content with the following:

# Reemplaza tu función obtener_declaracion_ruta completa con esta versión
@app.route('/api/declaracion_ruta/<string:cedula>', methods=['GET'])
def obtener_declaracion_ruta(cedula):
    """
    Obtiene la última declaración de ruta de un empleado por cédula,
    incluyendo nombre y apellido del empleado.
    """
    if not cedula:
        return jsonify({"error": "Cédula es requerida."}), 400

    conn = None
    cur = None
    try:
        conn = obtener_conexion_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                e.nombres,
                e.apellidos,
                dr.origen,
                dr.destino,
                dr.transporte_ida,
                dr.transporte_ida_otro,
                dr.hora_salida_ida, -- Selecciona la hora
                dr.transporte_regreso,
                dr.transporte_regreso_otro,
                dr.ruta_alterna_requerida,
                dr.ruta_alterna_descripcion,
                dr.fecha_declaracion
            FROM declaraciones_ruta dr -- ¡Nombre de tabla corregido aquí!
            JOIN empleados e ON dr.id_empleado = e.id_empleado
            WHERE e.cedula = %s
            ORDER BY dr.fecha_declaracion DESC
            LIMIT 1;
        """, (cedula,))
        declaracion = cur.fetchone()

        if not declaracion:
            # Si no se encuentra declaración de ruta, aún intentamos obtener nombre y apellido del empleado
            cur.execute("""
                SELECT nombres, apellidos FROM empleados WHERE cedula = %s;
            """, (cedula,))
            empleado_info = cur.fetchone()

            if not empleado_info:
                # Si ni siquiera se encuentra el empleado
                return jsonify({"message": "Empleado no encontrado."}), 404
            else:
                # Empleado encontrado, pero sin declaración de ruta para la fecha actual
                return jsonify({
                    "nombres": empleado_info[0],
                    "apellidos": empleado_info[1],
                    "message": "No se encontró declaración de ruta para esta cédula en la fecha actual."
                }), 200

        # Si se encontró una declaración
        # Convertir el objeto time a string HH:MM
        hora_salida_ida_str = declaracion[6].strftime('%H:%M') if declaracion[6] else None

        declaracion_data = {
            "nombres": declaracion[0],
            "apellidos": declaracion[1],
            "origen": declaracion[2],
            "destino": declaracion[3],
            "transporte_ida": declaracion[4],
            "transporte_ida_otro": declaracion[5],
            "hora_salida_ida": hora_salida_ida_str, # Aquí se envía la hora como string
            "transporte_regreso": declaracion[7],
            "transporte_regreso_otro": declaracion[8],
            "ruta_alterna_requerida": declaracion[9],
            "ruta_alterna_descripcion": declaracion[10],
            "fecha_declaracion": declaracion[11].isoformat() if declaracion[11] else None
        }
        return jsonify(declaracion_data), 200

    except Exception as e:
        app.logger.error(f"Error al obtener declaración de ruta: {e}")
        return jsonify({"error": "Error interno del servidor al obtener declaración de ruta."}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

# Start the Flask application
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9502, debug=True)
