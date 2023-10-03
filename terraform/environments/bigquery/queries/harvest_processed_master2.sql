SELECT
  h.id AS harvest_client_id,
  h.name AS harvest_client_contract_name,
  p.name AS pipedrive_client_name,
  h.is_active AS harvest_active,
  p.owner_name AS pipedrive_org_owner,
  p.id AS pipedrive_org_id,
  starts_on AS first_project_started,
  ends_on AS last_project_ends,
  sector,
  sub_sector,
  open_deals,
  won_deals,
  lost_deals,
  closed_deals,
  annual_revenue__m,
  employees,
  description,
  region___marketing,
  client_relationship_status,
  organisation_created
FROM
  `tpx-consulting-dashboards.Harvest_Raw.clients` AS h
RIGHT JOIN
  `tpx-consulting-dashboards.Pipedrive_Raw.organisations` AS p
ON
  h.address = CAST(p.id AS string)
LEFT JOIN (
  SELECT
    client_id,
    MIN(starts_on) AS starts_on,
    MAX(ends_on) AS ends_on
  FROM
    `tpx-cheetah.Harvest_Raw.projects`
  GROUP BY
    client_id) AS client_start_end_dates
ON
  client_start_end_dates.client_id = h.id
WHERE
  sector IS NOT null
