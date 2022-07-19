CREATE PROCEDURE order_status(
    in_c_w_id INT
  , in_c_d_id INT
  , in_c_id INT
  , in_c_last VARCHAR(24)
  , in_byname BOOL
) AS
$$
    let mut var_c_id = in_c_id;

    if in_byname {
        SELECT count(c_id) AS var_c_id_count
          FROM customer
         WHERE customer.c_w_id = in_c_w_id
           AND customer.c_d_id = in_c_d_id
           AND customer.c_last = in_c_last;

        let var_offset = CASE WHEN (var_c_id_count % 2) = 1 THEN var_c_id_count + 1 ELSE var_c_id_count END;

        SELECT c_id AS var_it
          FROM customer
         WHERE customer.c_w_id = in_c_w_id
           AND customer.c_d_id = in_c_d_id
           AND customer.c_last = in_c_last
        ORDER BY customer.c_first {
            var_c_id = var_it;
        } when no_data_found {
            rollback;
            return;
        }
    }

    SELECT
        c_first AS var_c_first
      , c_middle AS var_c_middle
      , c_last AS var_c_last
      , c_balance AS var_c_balance
    FROM customer
    WHERE customer.c_id = var_c_id
    AND customer.c_d_id = in_c_d_id
    AND customer.c_w_id = in_c_w_id;

    SELECT
        o_id AS var_o_id
      , o_entry_d AS var_o_entry_d
      , o_carrier_id AS var_o_carrier_id
    FROM orders
    WHERE orders.o_c_id = var_c_id
      AND orders.o_d_id = in_c_d_id
      AND orders.o_w_id = in_c_w_id
    ORDER BY o_id ASC
    LIMIT 1;

    SELECT
        ol_i_id AS var_o_i_id
      , ol_supply_w_id AS var_ol_supply_w_id
      , ol_quantity AS var_ol_quantity
      , ol_amount AS var_ol_amount
      , ol_delivery_d AS var_ol_delivery_d
    FROM order_line
    WHERE order_line.ol_o_id = var_o_id
      AND order_line.ol_d_id = in_c_d_id
      AND order_line.ol_w_id = in_c_w_id {
      -- do nothing, we just have to retrieve data
    }

    COMMIT;
$$ LANGUAGE umbrascript;
