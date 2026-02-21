/**
 * Graph Searcher — Link expansion retrieval + scoring.
 *
 * Given a user query, extracts entities, traverses the graph one hop,
 * and returns ranked exchange IDs for RRF fusion with continuity's
 * semantic and keyword search results.
 *
 * Phase 1: Single-hop link expansion. Phase 5 upgrades to multi-hop via recursive CTEs.
 */

const extractor = require('./extractor');

class GraphSearcher {
    /**
     * @param {GraphStore} store - Graph store instance
     * @param {Object} config - Retrieval config section
     */
    constructor(store, config) {
        this.store = store;
        this.config = config || {};

        // Prepared statements for search queries
        // These use dynamic SQL so we prepare them per-call, but we cache
        // the scoring logic here.
        this.maxResults = this.config.maxResults || 20;
        this.minSharedEntities = this.config.minSharedEntities || 1;
        this.cooccurrenceBoost = this.config.cooccurrenceBoost || 0.1;
    }

    /**
     * Run link expansion search for a query.
     *
     * @param {string} query - User's query text
     * @param {string} agentId - Agent ID
     * @param {Object} options
     * @param {number} options.limit - Max results
     * @param {Array} options.knownEntities - Known entities for gazetteer
     * @returns {{ exchanges: Array, entities: Array }}
     */
    search(query, agentId, options) {
        const opts = options || {};
        const aid = agentId || 'main';
        const limit = opts.limit || this.maxResults;

        // 1. Extract entities from the query
        const queryEntities = extractor.extractEntities(query, { minExchangeLength: 3 });

        // Also try gazetteer matching
        const gazetteered = extractor.matchGazetteer(query, opts.knownEntities || []);
        const entityNames = new Set(queryEntities.map(e => e.name.toLowerCase()));
        for (const g of gazetteered) {
            if (!entityNames.has(g.name.toLowerCase())) {
                queryEntities.push(g);
                entityNames.add(g.name.toLowerCase());
            }
        }

        if (queryEntities.length === 0) {
            return { exchanges: [], entities: [] };
        }

        // 2. For each query entity, find related exchanges in the graph
        const exchangeScores = new Map(); // exchange_id → { score, entities, confidence }

        for (const entity of queryEntities) {
            // Get all triples where this entity appears
            const triples = this.store.getTriplesFor(entity.name, aid, 100);

            for (const triple of triples) {
                if (!triple.source_exchange_id) continue;

                const exchangeId = triple.source_exchange_id;
                if (!exchangeScores.has(exchangeId)) {
                    exchangeScores.set(exchangeId, {
                        id: exchangeId,
                        score: 0,
                        sharedEntities: new Set(),
                        maxConfidence: 0,
                        newestDate: null
                    });
                }

                const entry = exchangeScores.get(exchangeId);
                entry.sharedEntities.add(entity.name.toLowerCase());
                entry.score += triple.confidence || 1.0;
                entry.maxConfidence = Math.max(entry.maxConfidence, triple.confidence || 1.0);
                if (!entry.newestDate || triple.source_date > entry.newestDate) {
                    entry.newestDate = triple.source_date;
                }
            }

            // 3. Co-occurrence expansion — find entities that frequently
            //    co-occur with the query entity, then look up THEIR exchanges
            const cooccurrences = this.store.getCooccurrences(entity.name, 5);

            for (const cooc of cooccurrences) {
                // The co-occurring entity is whichever side isn't our entity
                const normalizedName = this.store.normalizeEntityId(entity.name);
                const coocEntityId = cooc.entity_a === normalizedName ? cooc.entity_b : cooc.entity_a;

                // Look up the co-occurring entity's triples (limited scan)
                const coocTriples = this._getTriplesByNormalizedId(coocEntityId, aid, 20);

                for (const triple of coocTriples) {
                    if (!triple.source_exchange_id) continue;

                    const exchangeId = triple.source_exchange_id;
                    if (!exchangeScores.has(exchangeId)) {
                        exchangeScores.set(exchangeId, {
                            id: exchangeId,
                            score: 0,
                            sharedEntities: new Set(),
                            maxConfidence: 0,
                            newestDate: null
                        });
                    }

                    const entry = exchangeScores.get(exchangeId);
                    // Co-occurrence connections get a reduced boost
                    entry.score += this.cooccurrenceBoost * (cooc.count || 1);
                    if (!entry.newestDate || triple.source_date > entry.newestDate) {
                        entry.newestDate = triple.source_date;
                    }
                }
            }
        }

        // 4. Filter by minimum shared entities
        const results = [];
        for (const [exchangeId, entry] of exchangeScores) {
            if (entry.sharedEntities.size >= this.minSharedEntities) {
                results.push({
                    id: entry.id,
                    score: entry.score,
                    sharedEntityCount: entry.sharedEntities.size,
                    sharedEntities: Array.from(entry.sharedEntities),
                    maxConfidence: entry.maxConfidence,
                    date: entry.newestDate
                });
            }
        }

        // 5. Sort by score descending
        results.sort((a, b) => b.score - a.score);

        return {
            exchanges: results.slice(0, limit),
            entities: queryEntities
        };
    }

    /**
     * Look up triples by already-normalized entity ID.
     * Avoids double-normalization when coming from co-occurrence data.
     */
    _getTriplesByNormalizedId(normalizedId, agentId, limit) {
        const aid = agentId || 'main';
        const lim = limit || 50;

        // Direct SQL since the entity ID is already normalized
        const sql = `
            SELECT * FROM triples
            WHERE (subject = ? OR object = ?) AND agent_id = ?
            ORDER BY updated_at DESC
            LIMIT ?
        `;
        return this.store.db.prepare(sql).all(normalizedId, normalizedId, aid, lim);
    }

    /**
     * Get entity details + surrounding graph context.
     * Used by the graph.getEntity gateway method.
     */
    getEntityContext(entityName, agentId) {
        const aid = agentId || 'main';
        const id = this.store.normalizeEntityId(entityName);
        const entity = this.store.getEntity(id);
        if (!entity) return null;

        const triples = this.store.getTriplesFor(entityName, aid, 50);
        const cooccurrences = this.store.getCooccurrences(entityName, 10);

        // Group triples by predicate for structured output
        const relationships = {};
        for (const t of triples) {
            if (!relationships[t.predicate]) {
                relationships[t.predicate] = [];
            }
            relationships[t.predicate].push({
                subject: t.subject,
                object: t.object,
                confidence: t.confidence,
                date: t.source_date
            });
        }

        return {
            entity,
            relationships,
            cooccurrences: cooccurrences.map(c => ({
                entity: c.entity_a === id ? c.entity_b : c.entity_a,
                count: c.count,
                lastSeen: c.last_seen
            })),
            tripleCount: triples.length
        };
    }
}

module.exports = GraphSearcher;
