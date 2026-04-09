-- Reset base de datos de pruebas
-- Limpia todas las tablas transaccionales, conserva maestras:
-- catorcenas, cliente, criterios_autorizacion, equipo, espacio_inventario,
-- inventarios, usuario, usuario_equipo, proveedores, vistas

SET FOREIGN_KEY_CHECKS = 0;

-- Solicitudes y propuestas
TRUNCATE TABLE solicitud;
TRUNCATE TABLE solicitudCaras;
TRUNCATE TABLE solicitud_original;
TRUNCATE TABLE propuesta;
TRUNCATE TABLE cotizacion;

-- Campañas y reservas
TRUNCATE TABLE campania;
TRUNCATE TABLE reservas;

-- Tareas y comentarios
TRUNCATE TABLE tareas;
TRUNCATE TABLE comentarios;
TRUNCATE TABLE comentarios_revision_artes;

-- Historial
TRUNCATE TABLE historial;
TRUNCATE TABLE historial_comentarios;

-- Archivos y artes
TRUNCATE TABLE archivos;
TRUNCATE TABLE artes_tradicionales;
TRUNCATE TABLE imagenes_digitales;

-- Calendario (regenerable)
TRUNCATE TABLE calendario;

-- Tickets
TRUNCATE TABLE ticket_vistas;
TRUNCATE TABLE ticket_chat_vistas;
TRUNCATE TABLE ticket_mensajes;
TRUNCATE TABLE ticket_chat;
TRUNCATE TABLE tickets;

-- Chatbot
TRUNCATE TABLE chatbot_logs;

-- Correos y sesiones
TRUNCATE TABLE correos_enviados;
TRUNCATE TABLE session_locks;

-- Notas personales
TRUNCATE TABLE notas_personales;

SET FOREIGN_KEY_CHECKS = 1;

SELECT 'Tablas transaccionales limpiadas. Maestras intactas.' AS status;
