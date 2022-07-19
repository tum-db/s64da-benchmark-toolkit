CREATE PROCEDURE new_order(
    in_w_id INT
  , in_c_id INT
  , in_d_id INT
  , in_ol_cnt INT
  , in_all_local INT
  , in_itemids INT ARRAY
  , in_supware INT ARRAY
  , in_qty INT ARRAY
  , in_timestamp TIMESTAMPTZ
)
AS $$
    SELECT
      w_tax AS var_w_tax
    FROM warehouse
    WHERE warehouse.w_id = in_w_id;

    SELECT
        d_next_o_id AS var_d_next_o_id
      , d_tax AS var_d_tax
    FROM district
    WHERE district.d_id = in_d_id
      AND district.d_w_id = in_w_id;

    SELECT
        c_discount AS var_c_discount
      , c_last AS var_c_last
      , c_credit AS var_c_credit
    FROM customer
    WHERE customer.c_id = in_c_id
      AND customer.c_d_id = in_d_id
      AND customer.c_w_id = in_w_id;

    UPDATE district
    SET d_next_o_id = var_d_next_o_id + 1
    WHERE district.d_id = in_d_id
      AND district.d_w_id = in_w_id
    catch serialization_failure {
        return;
    }

    INSERT INTO orders(
        o_id
      , o_d_id
      , o_w_id
      , o_c_id
      , o_entry_d
      , o_ol_cnt
      , o_all_local
    ) VALUES (
        var_d_next_o_id
      , in_d_id
      , in_w_id
      , in_c_id
      , in_timestamp
      , in_ol_cnt
      , in_all_local
    )
    catch serialization_failure {
        return;
    }

    INSERT INTO new_orders(no_o_id, no_d_id, no_w_id)
    VALUES (var_d_next_o_id, in_d_id, in_w_id)
    catch serialization_failure {
        return;
    }

    SELECT ol_number AS var_ol_number FROM generate_series(1, in_ol_cnt) g(ol_number) {
        let var_ol_i_id = in_itemids[var_ol_number];
        let var_ol_supply_w_id = in_supware[var_ol_number];
        let var_ol_quantity = in_qty[var_ol_number];

        SELECT
            i_price AS var_i_price
          , i_name AS var_i_name
          , i_data AS var_i_data
        FROM item
        WHERE item.i_id = var_ol_i_id
        when no_data_found {
            rollback;
            return;
        }

        SELECT
            s_quantity AS var_s_quantity
          , CASE in_d_id
                 WHEN 1 THEN s_dist_01
                 WHEN 2 THEN s_dist_02
                 WHEN 3 THEN s_dist_03
                 WHEN 4 THEN s_dist_04
                 WHEN 5 THEN s_dist_05
                 WHEN 6 THEN s_dist_06
                 WHEN 7 THEN s_dist_07
                 WHEN 8 THEN s_dist_08
                 WHEN 9 THEN s_dist_09
                 WHEN 10 THEN s_dist_10
            END AS var_s_dist
          , s_ytd AS var_s_ytd
          , s_order_cnt AS var_s_order_cnt
          , s_remote_cnt AS var_s_remote_cnt
        FROM stock
        WHERE stock.s_w_id = var_ol_supply_w_id
          AND stock.s_i_id = var_ol_i_id;

        let var_s_new_quantity = CASE WHEN var_s_quantity >= var_ol_quantity + 10 then var_s_quantity - var_ol_quantity else var_s_quantity + 91 - var_ol_quantity END;
        let var_s_new_remote_cnt = var_s_remote_cnt + CASE WHEN var_ol_supply_w_id <> in_w_id THEN 1 ELSE 0 END;
        let var_s_new_order_cnt = var_s_order_cnt + 1;
        let var_s_new_ytd = var_s_ytd + var_ol_quantity;

        UPDATE stock
        SET s_quantity = var_s_new_quantity
          , s_order_cnt = var_s_new_order_cnt
          , s_remote_cnt = var_s_new_remote_cnt
          , s_ytd = var_s_new_ytd
        WHERE stock.s_w_id = var_ol_supply_w_id
          AND stock.s_i_id = var_ol_i_id
        catch serialization_failure {
            return;
        }

        let var_ol_amount = var_ol_quantity * var_i_price * (1 + var_w_tax + var_d_tax) * (1 - var_c_discount);

        INSERT INTO order_line(
            ol_o_id
          , ol_d_id
          , ol_w_id
          , ol_number
          , ol_i_id
          , ol_supply_w_id
          , ol_quantity
          , ol_amount
          , ol_dist_info
        ) VALUES (
            var_d_next_o_id
          , in_d_id
          , in_w_id
          , var_ol_number
          , var_ol_i_id
          , var_ol_supply_w_id
          , var_ol_quantity
          , var_ol_amount
          , var_s_dist
        )
        catch serialization_failure {
            return;
        }
    }

    COMMIT;
$$ LANGUAGE umbrascript;
