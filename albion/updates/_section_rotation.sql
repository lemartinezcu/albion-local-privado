-- Añade el ángulo de rotación para secciones (en grados)
ALTER TABLE _albion.metadata
ADD COLUMN IF NOT EXISTS section_rotation_deg real DEFAULT 0;

-- Expone la nueva columna en la vista pública
CREATE OR REPLACE VIEW albion.metadata AS
SELECT
  id, srid, close_collar_distance, snap_distance, precision,
  interpolation, end_node_relative_distance, end_node_relative_thickness,
  correlation_distance, correlation_angle, parent_correlation_angle,
  max_snapping_distance, section_rotation_deg
FROM _albion.metadata;