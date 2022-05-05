-- remove repeated targets, the older ones are not useful anymore. targets with
-- weight 0 will be kept, as we cannot tell which were deleted and which were
-- explicitly set as 0.
local function c_remove_unused_targets(coordinator)
  local cassandra = require "cassandra"
  local upstream_targets = {}
  for rows, err in coordinator:iterate("SELECT id, upstream_id, target, created_at FROM targets") do
    if err then
      return nil, err
    end

    for _, row in ipairs(rows) do
      local key = string.format("%s:%s", row.upstream_id, row.target)

      if not upstream_targets[key] then
        upstream_targets[key] = {
          id = row.id,
          created_at = row.created_at,
        }
      else
        local to_remove
        if row.created_at > upstream_targets[key].created_at then
          to_remove = upstream_targets[key].id
          upstream_targets[key] = {
            id = row.id,
            created_at = row.created_at,
          }
        else
          to_remove = row.id
        end
        local _, err = coordinator:execute("DELETE FROM targets WHERE id = ?", {
          cassandra.uuid(to_remove)
        })

        if err then
          return nil, err
        end
      end
    end
  end

  return true
end


-- update cache_key for targets
local function c_update_target_cache_key(coordinator)
  local cassandra = require "cassandra"
  for rows, err in coordinator:iterate("SELECT id, upstream_id, target, ws_id FROM targets") do
    if err then
      return nil, err
    end

    for _, row in ipairs(rows) do
      local cache_key = string.format("targets:%s:%s::::%s", row.upstream_id, row.target, row.ws_id)

      local _, err = coordinator:execute("UPDATE targets SET cache_key = ? WHERE id = ? IF EXISTS", {
        cache_key, cassandra.uuid(row.id)
      })

      if err then
        return nil, err
      end
    end
  end

  return true
end


return {
  postgres = {
    up = [[
      DO $$
      BEGIN
        -- we only want to run this migration if there is vaults_beta table
        IF (SELECT to_regclass('vaults_beta')) IS NOT NULL THEN
          DROP TRIGGER IF EXISTS "vaults_beta_sync_tags_trigger" ON "vaults_beta";

          -- Enterprise Edition has a Vaults table created by a Vault Auth Plugin
          ALTER TABLE IF EXISTS ONLY "vaults" RENAME TO "vault_auth_vaults";
          ALTER TABLE IF EXISTS ONLY "vault_auth_vaults" RENAME CONSTRAINT "vaults_pkey" TO "vault_auth_vaults_pkey";
          ALTER TABLE IF EXISTS ONLY "vault_auth_vaults" RENAME CONSTRAINT "vaults_name_key" TO "vault_auth_vaults_name_key";

          ALTER TABLE IF EXISTS ONLY "vaults_beta" RENAME TO "vaults";
          ALTER TABLE IF EXISTS ONLY "vaults" RENAME CONSTRAINT "vaults_beta_pkey" TO "vaults_pkey";
          ALTER TABLE IF EXISTS ONLY "vaults" RENAME CONSTRAINT "vaults_beta_id_ws_id_key" TO "vaults_id_ws_id_key";
          ALTER TABLE IF EXISTS ONLY "vaults" RENAME CONSTRAINT "vaults_beta_prefix_key" TO "vaults_prefix_key";
          ALTER TABLE IF EXISTS ONLY "vaults" RENAME CONSTRAINT "vaults_beta_prefix_ws_id_key" TO "vaults_prefix_ws_id_key";
          ALTER TABLE IF EXISTS ONLY "vaults" RENAME CONSTRAINT "vaults_beta_ws_id_fkey" TO "vaults_ws_id_fkey";

          ALTER INDEX IF EXISTS "vaults_beta_tags_idx" RENAME TO "vaults_tags_idx";

          BEGIN
            CREATE TRIGGER "vaults_sync_tags_trigger"
            AFTER INSERT OR UPDATE OF "tags" OR DELETE ON "vaults"
            FOR EACH ROW
            EXECUTE PROCEDURE sync_tags();
          EXCEPTION WHEN UNDEFINED_COLUMN OR UNDEFINED_TABLE THEN
            -- Do nothing, accept existing state
          END;
        END IF;
      END$$;

      DO $$
        BEGIN
          ALTER TABLE IF EXISTS ONLY "targets" ADD COLUMN "cache_key" TEXT UNIQUE;
        EXCEPTION WHEN duplicate_column THEN
          -- Do nothing, accept existing state
        END;
      $$;
    ]],
    teardown = function(connector)
      local _, err = connector:query([[
        DELETE FROM targets t1
              USING targets t2
              WHERE t1.created_at < t2.created_at
                AND t1.upstream_id = t2.upstream_id
                AND t1.target = t2.target;
        UPDATE targets SET cache_key = CONCAT('targets:', upstream_id, ':', target, '::::', ws_id);
        ]])

      if err then
        return nil, err
      end

      return true
    end
  },

  cassandra = {
    up = [[
      CREATE TABLE IF NOT EXISTS vault_auth_vaults (
        id          uuid,
        created_at  timestamp,
        updated_at  timestamp,
        name        text,
        protocol    text,
        host        text,
        port        int,
        mount       text,
        vault_token text,
        PRIMARY KEY (id)
      );

      ALTER TABLE targets ADD cache_key text;
      CREATE INDEX IF NOT EXISTS targets_cache_key_idx ON targets(cache_key);
    ]],
    teardown = function(connector)
      local coordinator = assert(connector:get_stored_connection())
      local _, err = c_remove_unused_targets(coordinator)
      if err then
        return nil, err
      end

      _, err = c_update_target_cache_key(coordinator)
      if err then
        return nil, err
      end

      return true
    end
  },
}
