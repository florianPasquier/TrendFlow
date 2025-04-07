
-- ### Nouveaux Produits (New Products)

-- | **Colonne**            | **Type**        | **Description**                                                        |
-- |------------------------|-----------------|------------------------------------------------------------------------|
-- | `launch_date`          | `DATE`          | La date de lancement du produit (format : `YYYY-MM-DD`).               |
-- | `product_id`           | `STRING`        | L'ID unique du produit.                                                |
-- | `product_name`         | `STRING`        | Le nom du produit.                                                     |
-- | `category_id`          | `INTEGER`       | L'ID de la catégorie à laquelle appartient le produit.                 |
-- | `price`                | `FLOAT`         | Le prix du produit.                                                    |
-- | `supplier_id`          | `STRING`        | L'ID du fournisseur du produit.                                        |
-- | `product_description`  | `TEXT`          | Une description textuelle du produit.                                  |
-- | `region`               | `STRING`        | La région où le produit est lancé.    

-- sample data :

-- 2024-01-01	104	"Product X"	1	25.0	201	"Tech gadget for home use"	Europe
-- 2024-01-02	105	"Product Y"	2	30.0	202	"Wearable health device"	US
-- 2024-01-03	106	"Product Z"	3	15.0	203	"Portable speaker for outdoor use"	Europe


-- dbt model for new products
with new_products as (
    select
        launch_date,
        product_id,
        product_name,
        category_id,
        price,
        supplier_id,
        product_description,
        region
    from {{ ref('udata_new_products') }}
)

select
    launch_date,
    product_id,
    product_name,
    category_id,
    price,
    supplier_id,
    product_description,
    region
from new_products

