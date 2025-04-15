USE vietnamese_administrative_units
SELECT 
	ROW_NUMBER() OVER (ORDER BY adr.id, adu.id) -1 AS id,
	w.full_name AS [wards],
	d.full_name AS [districts], 
	p.full_name AS [provinces]
INTO address
FROM wards AS w
JOIN districts AS d ON district_code = d.code
JOIN provinces AS p ON d.province_code = p.code
JOIN administrative_units AS adu ON adu.id = p.administrative_unit_id
JOIN administrative_regions AS adr ON adr.id = p.administrative_region_id;