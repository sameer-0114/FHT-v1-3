-- Optional: enable NOTIFY on insert/update for timeframe_data.
-- Adjust channel names if you prefer namespacing by symbol as well.

-- Indexes to speed up common queries
CREATE INDEX IF NOT EXISTS idx_timeframe_data_tf_sym_bucket
  ON timeframe_data (timeframe, symbol, bucket DESC);

-- Trigger function to notify per-timeframe channel
CREATE OR REPLACE FUNCTION notify_timeframe_change() RETURNS trigger AS $$
DECLARE
  chan text;
BEGIN
  chan := 'bars_' || NEW.timeframe;
  PERFORM pg_notify(chan, NEW.symbol || ':' || NEW.timeframe || ':' || NEW.bucket::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach to inserts and updates that change OHLC, close, or volume
DROP TRIGGER IF EXISTS trg_notify_timeframe_change ON timeframe_data;
CREATE TRIGGER trg_notify_timeframe_change
AFTER INSERT OR UPDATE OF open_price, high_price, low_price, close_price, total_volume
ON timeframe_data
FOR EACH ROW
WHEN (NEW.symbol IS NOT NULL AND NEW.timeframe IS NOT NULL)
EXECUTE FUNCTION notify_timeframe_change();
