DROP PROCEDURE IF EXISTS stock_level(INT, INT, INT);
CREATE PROCEDURE stock_level(
    in_w_id INT
  , in_d_id INT
  , in_threshold INT
) AS $$
DECLARE
  this_d_next_o_id INT;
  low_stock_count INT;
BEGIN
  BEGIN
      SELECT d_next_o_id
      FROM district
      WHERE d_id = in_d_id
        AND d_w_id = in_w_id
      INTO this_d_next_o_id;

      SELECT COUNT(DISTINCT(s_i_id))
      FROM order_line, stock
      WHERE ol_w_id = in_w_id
        AND ol_d_id = in_d_id
        AND ol_o_id <  this_d_next_o_id
        AND ol_o_id >= this_d_next_o_id
        AND s_w_id = in_w_id
        AND s_i_id = ol_i_id
        AND s_quantity < in_threshold
      INTO low_stock_count;
    EXCEPTION
        WHEN serialization_failure OR deadlock_detected OR no_data_found
            THEN
                ROLLBACK;
    END;

    COMMIT;
END;
$$ LANGUAGE plpgsql;
