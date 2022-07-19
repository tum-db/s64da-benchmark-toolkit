CREATE PROCEDURE delivery(
    in_w_id INT
  , in_o_carrier_id INT
  , in_dist_per_ware INT
  , in_timestamp TIMESTAMPTZ
) AS $$
    SELECT d_id AS var_d_id FROM generate_series(1, in_dist_per_ware) g(d_id) {
        SELECT no_o_id AS var_no_o_id
        FROM new_orders
        WHERE new_orders.no_w_id = in_w_id
          AND new_orders.no_d_id = CAST(var_d_id AS INTEGER)
        ORDER BY no_o_id ASC
        LIMIT 1
        when no_data_found {
            continue;
        }

        DELETE
        FROM new_orders
        WHERE new_orders.no_o_id = var_no_o_id
          AND new_orders.no_d_id = CAST (var_d_id AS INTEGER)
          AND new_orders.no_w_id = in_w_id
        catch serialization_failure {
            return;
        }

        SELECT o_c_id AS var_o_c_id
        FROM orders
        WHERE orders.o_id = var_no_o_id
          AND orders.o_d_id = CAST (var_d_id AS INTEGER)
          AND orders.o_w_id = in_w_id;

        UPDATE orders
        SET o_carrier_id = in_o_carrier_id
        WHERE orders.o_id = var_no_o_id
          AND orders.o_d_id = CAST (var_d_id AS INTEGER)
          AND orders.o_w_id = in_w_id
        catch serialization_failure {
            return;
        }

        let mut var_ol_total : NUMERIC(6, 2) = 0;

        SELECT ol_amount AS var_ol_amount
        FROM order_line
        WHERE order_line.ol_o_id = var_no_o_id
          AND order_line.ol_d_id = CAST (var_d_id AS INTEGER)
          AND order_line.ol_w_id = in_w_id {
            var_ol_total = var_ol_total + var_ol_amount;
        }

        UPDATE order_line
        SET ol_delivery_d = in_timestamp
        WHERE order_line.ol_o_id = var_no_o_id
          AND order_line.ol_d_id = CAST (var_d_id AS INTEGER)
          AND order_line.ol_w_id = in_w_id
        catch serialization_failure {
            return;
        }

        UPDATE customer
        SET c_balance = c_balance + var_ol_total,
            c_delivery_cnt = c_delivery_cnt + 1
        WHERE customer.c_id = var_o_c_id
          AND customer.c_d_id = CAST (var_d_id AS INTEGER)
          AND customer.c_w_id = in_w_id
        catch serialization_failure {
            return;
        }
    }

    COMMIT;
$$ LANGUAGE umbrascript;
