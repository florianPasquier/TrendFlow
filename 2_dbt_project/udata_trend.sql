-- ### Données de Tendances (Trend Data)

-- | **Colonne**        | **Type**        | **Description**                                                        |
-- |--------------------|-----------------|------------------------------------------------------------------------|
-- | `date`             | `DATE`          | La date à laquelle la tendance a été observée.                          |
-- | `trend_score`      | `INTEGER`       | Score ou niveau d'intérêt pour un produit sur la plateforme de tendance (par exemple, nombre de recherches ou mentions). |
-- | `keyword`          | `STRING`        | Le mot-clé ou la tendance associée (par exemple, le nom d’un produit ou catégorie). |
-- | `platform`         | `STRING`        | La plateforme d'où provient la tendance (par exemple, `Google Trends`, `TikTok`). |
-- | `region`           | `STRING`        | La région géographique de la tendance (utile pour les tendances régionales). |


-- sample data : 
-- date	trend_score	keyword	platform	region
-- 2024-01-01	150	"Product A"	Google Trends	Europe
-- 2024-01-02	200	"Product B"	TikTok	US
-- 2024-01-03	180	"Product A"	Google Trends	Europe

-- dbt model for trend data
with trend_data as (
    select
        date,
        trend_score,
        keyword,
        platform,
        region
    from {{ ref('udata_trend_data') }}
)

select
    date,
    trend_score,
    keyword,
    platform,
    region
from trend_data

