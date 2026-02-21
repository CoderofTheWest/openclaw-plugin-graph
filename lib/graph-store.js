/**
 * Graph Store — SQLite-backed triple store with entity registry and co-occurrence cache.
 *
 * Separate graph.db file per agent. Different write patterns from continuity.db
 * (many small writes from entity extraction vs. bulk writes from archiving),
 * so separate WAL journals prevent contention.
 */

const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

const SCHEMA_SQL = `
-- Core triples (subject → predicate → object)
CREATE TABLE IF NOT EXISTS triples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject TEXT NOT NULL,
    predicate TEXT NOT NULL,
    object TEXT NOT NULL,
    confidence REAL DEFAULT 1.0,
    source_exchange_id TEXT,
    source_date TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    agent_id TEXT DEFAULT 'main',
    pending_resolution INTEGER DEFAULT 0
);

-- Entity registry (canonical forms + metadata)
CREATE TABLE IF NOT EXISTS entities (
    id TEXT PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    entity_type TEXT DEFAULT 'CONCEPT',
    first_seen TEXT DEFAULT (datetime('now')),
    last_seen TEXT DEFAULT (datetime('now')),
    mention_count INTEGER DEFAULT 1,
    aliases TEXT DEFAULT '[]',
    metadata TEXT,
    agent_id TEXT DEFAULT 'main'
);

-- Entity co-occurrence cache
CREATE TABLE IF NOT EXISTS cooccurrences (
    entity_a TEXT NOT NULL,
    entity_b TEXT NOT NULL,
    count INTEGER DEFAULT 1,
    last_seen TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (entity_a, entity_b)
);

-- Indexes for efficient traversal
CREATE INDEX IF NOT EXISTS idx_triples_subject ON triples(subject, agent_id);
CREATE INDEX IF NOT EXISTS idx_triples_object ON triples(object, agent_id);
CREATE INDEX IF NOT EXISTS idx_triples_predicate ON triples(predicate, agent_id);
CREATE INDEX IF NOT EXISTS idx_triples_subj_pred ON triples(subject, predicate);
CREATE INDEX IF NOT EXISTS idx_triples_obj_pred ON triples(object, predicate);
CREATE INDEX IF NOT EXISTS idx_triples_source ON triples(source_exchange_id);
CREATE INDEX IF NOT EXISTS idx_triples_date ON triples(source_date);
CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type, agent_id);
CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(canonical_name);
`;

class GraphStore {
    constructor(dbPath) {
        const dir = path.dirname(dbPath);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }

        this.db = new Database(dbPath);
        this.db.pragma('journal_mode = WAL');
        this.db.pragma('busy_timeout = 5000');
        this.db.exec(SCHEMA_SQL);

        // Prepared statements
        this._insertTriple = this.db.prepare(`
            INSERT INTO triples (subject, predicate, object, confidence, source_exchange_id, source_date, agent_id, pending_resolution)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `);

        this._findTriple = this.db.prepare(`
            SELECT id FROM triples
            WHERE subject = ? AND predicate = ? AND object = ? AND agent_id = ?
            LIMIT 1
        `);

        this._updateTripleConfidence = this.db.prepare(`
            UPDATE triples SET confidence = MAX(confidence, ?), updated_at = datetime('now')
            WHERE id = ?
        `);

        this._upsertEntity = this.db.prepare(`
            INSERT INTO entities (id, canonical_name, entity_type, agent_id, aliases)
            VALUES (?, ?, ?, ?, '[]')
            ON CONFLICT(id) DO UPDATE SET
                last_seen = datetime('now'),
                mention_count = mention_count + 1
        `);

        this._getEntity = this.db.prepare(`
            SELECT * FROM entities WHERE id = ?
        `);

        this._getEntityByName = this.db.prepare(`
            SELECT * FROM entities WHERE canonical_name = ? AND agent_id = ? LIMIT 1
        `);

        this._findEntitiesByPrefix = this.db.prepare(`
            SELECT * FROM entities WHERE canonical_name LIKE ? AND agent_id = ? LIMIT 10
        `);

        this._upsertCooccurrence = this.db.prepare(`
            INSERT INTO cooccurrences (entity_a, entity_b, count, last_seen)
            VALUES (?, ?, 1, datetime('now'))
            ON CONFLICT(entity_a, entity_b) DO UPDATE SET
                count = count + 1,
                last_seen = datetime('now')
        `);

        this._getCooccurrences = this.db.prepare(`
            SELECT * FROM cooccurrences
            WHERE entity_a = ? OR entity_b = ?
            ORDER BY count DESC
            LIMIT ?
        `);

        this._getTriplesForSubject = this.db.prepare(`
            SELECT * FROM triples WHERE subject = ? AND agent_id = ?
            ORDER BY updated_at DESC LIMIT ?
        `);

        this._getTriplesForObject = this.db.prepare(`
            SELECT * FROM triples WHERE object = ? AND agent_id = ?
            ORDER BY updated_at DESC LIMIT ?
        `);

        this._getTriplesForExchange = this.db.prepare(`
            SELECT * FROM triples WHERE source_exchange_id = ?
        `);

        this._countEntities = this.db.prepare(`
            SELECT COUNT(*) as count FROM entities WHERE agent_id = ?
        `);

        this._countTriples = this.db.prepare(`
            SELECT COUNT(*) as count FROM triples WHERE agent_id = ?
        `);

        this._recentEntities = this.db.prepare(`
            SELECT * FROM entities WHERE agent_id = ?
            ORDER BY last_seen DESC LIMIT ?
        `);

        this._topCooccurrences = this.db.prepare(`
            SELECT * FROM cooccurrences ORDER BY count DESC LIMIT ?
        `);

        this._deleteTriplesByExchange = this.db.prepare(`
            DELETE FROM triples WHERE source_exchange_id = ?
        `);
    }

    /**
     * Normalize an entity name to a canonical ID.
     * Phase 1: lowercase + trim. Phase 3 upgrades to fuzzy matching.
     */
    normalizeEntityId(name) {
        return name.toLowerCase().trim().replace(/\s+/g, '_');
    }

    /**
     * Add or update an entity in the registry.
     */
    upsertEntity(name, type, agentId) {
        const id = this.normalizeEntityId(name);
        this._upsertEntity.run(id, name, type || 'CONCEPT', agentId || 'main');
        return id;
    }

    /**
     * Look up an entity by normalized ID.
     */
    getEntity(id) {
        return this._getEntity.get(id);
    }

    /**
     * Look up an entity by canonical name + agent.
     */
    getEntityByName(name, agentId) {
        return this._getEntityByName.get(name, agentId || 'main');
    }

    /**
     * Find entities matching a prefix (for gazetteer matching).
     */
    findEntitiesByPrefix(prefix, agentId) {
        return this._findEntitiesByPrefix.all(prefix + '%', agentId || 'main');
    }

    /**
     * Resolve a mention to a canonical entity ID.
     * Phase 1: exact match only (lowercase normalized).
     * Returns { id, isNew } — isNew if entity was just created.
     */
    resolveEntity(name, type, agentId) {
        const id = this.normalizeEntityId(name);
        const existing = this._getEntity.get(id);
        this.upsertEntity(name, type, agentId);
        return { id, isNew: !existing };
    }

    /**
     * Add a triple to the graph. Deduplicates: if the same
     * (subject, predicate, object) exists, bumps confidence.
     */
    addTriple({ subject, predicate, object, confidence, sourceExchangeId, sourceDate, agentId, pendingResolution }) {
        const subjectId = this.normalizeEntityId(subject);
        const objectId = this.normalizeEntityId(object);
        const aid = agentId || 'main';

        const existing = this._findTriple.get(subjectId, predicate, objectId, aid);
        if (existing) {
            this._updateTripleConfidence.run(confidence || 1.0, existing.id);
            return existing.id;
        }

        const result = this._insertTriple.run(
            subjectId,
            predicate,
            objectId,
            confidence || 1.0,
            sourceExchangeId || null,
            sourceDate || new Date().toISOString().split('T')[0],
            aid,
            pendingResolution ? 1 : 0
        );
        return result.lastInsertRowid;
    }

    /**
     * Write a batch of triples + entities from one exchange.
     * Wraps in a transaction for atomicity.
     */
    writeExchange({ entities, triples, cooccurrences, agentId, sourceExchangeId, sourceDate }) {
        const aid = agentId || 'main';
        const date = sourceDate || new Date().toISOString().split('T')[0];

        const tx = this.db.transaction(() => {
            // Register entities
            for (const entity of (entities || [])) {
                this.upsertEntity(entity.name, entity.type, aid);
            }

            // Write triples
            const tripleIds = [];
            for (const triple of (triples || [])) {
                const id = this.addTriple({
                    subject: triple.subject,
                    predicate: triple.predicate,
                    object: triple.object,
                    confidence: triple.confidence,
                    sourceExchangeId,
                    sourceDate: date,
                    agentId: aid
                });
                tripleIds.push(id);
            }

            // Update co-occurrence cache
            for (const [a, b] of (cooccurrences || [])) {
                const aId = this.normalizeEntityId(a);
                const bId = this.normalizeEntityId(b);
                // Always store in sorted order for consistency
                const sorted = [aId, bId].sort();
                this._upsertCooccurrence.run(sorted[0], sorted[1]);
            }

            return tripleIds;
        });

        return tx();
    }

    /**
     * Get triples where an entity appears as subject or object.
     */
    getTriplesFor(entityName, agentId, limit) {
        const id = this.normalizeEntityId(entityName);
        const aid = agentId || 'main';
        const lim = limit || 50;

        const asSubject = this._getTriplesForSubject.all(id, aid, lim);
        const asObject = this._getTriplesForObject.all(id, aid, lim);

        // Deduplicate by triple ID
        const seen = new Set();
        const result = [];
        for (const t of [...asSubject, ...asObject]) {
            if (!seen.has(t.id)) {
                seen.add(t.id);
                result.push(t);
            }
        }
        return result;
    }

    /**
     * Get co-occurring entities for a given entity.
     */
    getCooccurrences(entityName, limit) {
        const id = this.normalizeEntityId(entityName);
        return this._getCooccurrences.all(id, id, limit || 20);
    }

    /**
     * Get graph stats for an agent.
     */
    getStats(agentId) {
        const aid = agentId || 'main';
        return {
            entityCount: this._countEntities.get(aid).count,
            tripleCount: this._countTriples.get(aid).count,
            recentEntities: this._recentEntities.all(aid, 10),
            topCooccurrences: this._topCooccurrences.all(10)
        };
    }

    /**
     * Query triples with optional filters.
     */
    queryTriples({ subject, predicate, object, agentId, limit }) {
        const conditions = [];
        const params = [];

        if (subject) { conditions.push('subject = ?'); params.push(this.normalizeEntityId(subject)); }
        if (predicate) { conditions.push('predicate = ?'); params.push(predicate); }
        if (object) { conditions.push('object = ?'); params.push(this.normalizeEntityId(object)); }
        if (agentId) { conditions.push('agent_id = ?'); params.push(agentId); }

        const where = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';
        const sql = `SELECT * FROM triples ${where} ORDER BY updated_at DESC LIMIT ?`;
        params.push(limit || 50);

        return this.db.prepare(sql).all(...params);
    }

    /**
     * Delete triples for a given exchange (used when re-extracting).
     */
    deleteTriplesByExchange(exchangeId) {
        return this._deleteTriplesByExchange.run(exchangeId);
    }

    close() {
        this.db.close();
    }
}

module.exports = GraphStore;
