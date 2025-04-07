-- ### Historique des ventes (Sales History)

-- | **Colonne**        | **Type**        | **Description**                                                        |
-- |--------------------|-----------------|------------------------------------------------------------------------|
-- | `date`             | `DATE`          | La date de la vente (format : `YYYY-MM-DD`).                           |
-- | `product_id`       | `STRING`        | L'ID unique du produit vendu.                                           |
-- | `sales`            | `INTEGER`       | Nombre d'unités vendues ou chiffre d'affaires pour la journée.          |
-- | `price`            | `FLOAT`         | Le prix unitaire du produit vendu.                                      |
-- | `category_id`      | `INTEGER`       | L'ID de la catégorie du produit.                                        |
-- | `region`           | `STRING`        | La région géographique de la vente (par exemple, `Europe`, `US`).       |
-- | `discount`         | `FLOAT`         | Taux de réduction appliqué au produit (ex. 0.10 pour 10%).              |
-- | `inventory_level`  | `INTEGER`       | Niveau de stock disponible du produit au moment de la vente.            |

-- date	product_id	sales	price	category_id	region	discount	inventory_level
-- 2024-01-01	101	50	20.0	1	Europe	0.10	200
-- 2024-01-02	102	30	35.0	2	US	0.05	150
-- 2024-01-03	103	40	25.0	1	Europe	0.15	180

-- dbt model for sales history
with sales_history as (
    select
        date,
        product_id,
        sales,
        price,
        category_id,
        region,
        discount,
        inventory_level
    from {{ ref('udata_sales_history') }}
)

select
    date,
    product_id,
    sales,
    price,
    category_id,
    region,
    discount,
    inventory_level 
from sales_history





