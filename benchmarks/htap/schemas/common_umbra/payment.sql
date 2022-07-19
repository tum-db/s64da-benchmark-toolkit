CREATE PROCEDURE payment(
    in_w_id INT
  , in_d_id INT
  , in_c_d_id INT
  , in_c_id INT
  , in_c_w_id INT
  , in_h_amount NUMERIC(12,2)
  , in_byname BOOL
  , in_c_last CHARACTER VARYING(16)
  , in_timestamp TIMESTAMPTZ
) AS $$
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
        w_name AS var_w_name
      , w_street_1 AS var_w_street_1
      , w_street_2 AS var_w_street_2
      , w_city AS var_w_city
      , w_state AS var_w_state
      , w_zip AS var_w_zip
      , w_ytd AS var_w_ytd
    FROM warehouse
    WHERE warehouse.w_id = in_w_id;

    let var_w_new_ytd = var_w_ytd + in_h_amount;

    UPDATE warehouse
    SET w_ytd = var_w_new_ytd
    WHERE warehouse.w_id = in_w_id
    catch serialization_failure {
        return;
    }

    SELECT
        d_name AS var_d_name
      , d_street_1 AS var_d_street_1
      , d_street_2 AS var_d_street_2
      , d_city AS var_d_city
      , d_state AS var_d_state
      , d_zip AS var_d_zip
      , d_ytd AS var_d_ytd
    FROM district
    WHERE district.d_id = in_d_id
      AND district.d_w_id = in_w_id;

    let var_d_new_ytd = var_d_ytd + in_h_amount;

    UPDATE district
    SET d_ytd = var_d_new_ytd
    WHERE district.d_id = in_d_id
      AND district.d_w_id = in_w_id
    catch serialization_failure {
        return;
    }

    SELECT
        c_first AS var_c_first
      , c_middle AS var_c_middle
      , c_last AS var_c_last
      , c_street_1 AS var_c_street_1
      , c_street_2 AS var_c_street_2
      , c_city AS var_c_city
      , c_state AS var_c_state
      , c_zip AS var_c_zip
      , c_phone AS var_c_phone
      , c_since AS var_c_since
      , c_credit AS var_c_credit
      , c_credit_lim AS var_c_credit_lim
      , c_discount AS var_c_discount
      , c_balance AS var_c_balance
      , c_ytd_payment AS var_c_ytd_payment
      , c_payment_cnt AS var_c_payment_cnt
    FROM customer
    WHERE customer.c_id = var_c_id
      AND customer.c_d_id = in_c_d_id
      AND customer.c_w_id = in_c_w_id;

    let var_c_new_balance = var_c_balance - in_h_amount;
    let var_c_new_ytd_payment = var_c_ytd_payment + in_h_amount;
    let var_c_new_payment_cnt = var_c_payment_cnt + 1;

    if var_c_credit = 'BC' {
        SELECT c_data AS var_c_data
        FROM customer
        WHERE customer.c_id = var_c_id
          AND customer.c_d_id = in_c_d_id
          AND customer.c_w_id = in_c_w_id;

        let var_c_new_data = var_c_data || var_c_id::TEXT || in_c_d_id::TEXT || in_c_w_id::TEXT || in_d_id::TEXT || in_w_id::TEXT || in_h_amount::TEXT || in_timestamp::TEXT || var_w_name::TEXT || var_d_name::TEXT;

        UPDATE customer
        SET c_balance = var_c_new_balance,
            c_ytd_payment = var_c_new_ytd_payment,
            c_payment_cnt = var_c_new_payment_cnt,
            c_data = substring(var_c_new_data from 1 for 500)
        WHERE customer.c_id = var_c_id
          AND customer.c_d_id = in_c_d_id
          AND customer.c_w_id = in_c_w_id
        catch serialization_failure {
            return;
        }
    } else {
        UPDATE customer
        SET c_balance = var_c_new_balance,
            c_ytd_payment = var_c_new_ytd_payment,
            c_payment_cnt = var_c_new_payment_cnt
        WHERE customer.c_id = var_c_id
          AND customer.c_d_id = in_c_d_id
          AND customer.c_w_id = in_c_w_id
        catch serialization_failure {
            return;
        }
    }

    INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data)
    VALUES (var_c_id, in_c_d_id, in_c_w_id, in_d_id, in_w_id, in_timestamp, in_h_amount, var_w_name || ' ' || var_d_name)
    catch serialization_failure {
        return;
    }

    COMMIT;
$$ LANGUAGE umbrascript;
