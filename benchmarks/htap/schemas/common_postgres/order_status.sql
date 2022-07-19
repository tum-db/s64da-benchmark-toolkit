DROP PROCEDURE IF EXISTS order_status(INT, INT, INT, VARCHAR, BOOL);
CREATE PROCEDURE order_status(
    in_c_w_id INT
  , in_c_d_id INT
  , in_c_id INT
  , in_c_last VARCHAR(24)
  , in_byname BOOL
) AS $$
DECLARE
  namecnt BIGINT;
  customer_rec RECORD;
  order_rec RECORD;
  order_line_rec RECORD;
BEGIN
  BEGIN
      IF in_byname THEN
        SELECT count(c_id)
        FROM customer
        WHERE c_w_id = in_c_w_id
          AND c_d_id = in_c_d_id
          AND c_last = in_c_last
        INTO namecnt;

        IF namecnt % 2 = 1 THEN
          namecnt = namecnt + 1;
        END IF;

        SELECT c_balance, c_first, c_middle, c_last, c_id
        FROM customer
        WHERE c_w_id = in_c_w_id
          AND c_d_id = in_c_d_id
          AND c_last = in_c_last
        ORDER BY c_first
        OFFSET namecnt / 2
        LIMIT 1
        INTO customer_rec;
      ELSE
        SELECT c_balance, c_first, c_middle, c_last, c_id
        FROM customer
        WHERE c_w_id = in_c_w_id
          AND c_d_id = in_c_d_id
          AND c_id = in_c_id
        INTO customer_rec;
      END IF;

      SELECT o_id, o_entry_d, o_carrier_id
      FROM orders
      WHERE o_w_id = in_c_w_id
        AND o_d_id = in_c_d_id
        AND o_c_id = in_c_id
        AND o_id = (
          SELECT max(o_id)
          FROM orders
          WHERE o_w_id = in_c_w_id
            AND o_d_id = in_c_d_id
            AND o_c_id = in_c_id
        )
      INTO order_rec;
    EXCEPTION
        WHEN serialization_failure OR deadlock_detected OR no_data_found
            THEN
                ROLLBACK;
    END;

    COMMIT;
END
$$ LANGUAGE PLPGSQL;
