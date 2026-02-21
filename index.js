/**
 * openclaw-plugin-graph — Knowledge Graph Plugin
 *
 * Entity extraction, triple storage, and graph-based retrieval for OpenClaw agents.
 * Phase 1: compromise.js NER + SQLite triples + single-hop link expansion.
 */

const fs = require('fs');
const path = require('path');
const GraphStore = require('./lib/graph-store');
const GraphSearcher = require('./lib/graph-searcher');
const extractor = require('./lib/extractor');

function deepMerge(target, source) {
    const result = { ...target };
    for (const key of Object.keys(source || {})) {
        const next = source[key];
        if (next && typeof next === 'object' && !Array.isArray(next)) {
            result[key] = deepMerge(result[key] || {}, next);
        } else {
            result[key] = next;
        }
    }
    return result;
}

function loadConfig(userConfig) {
    const defaults = JSON.parse(fs.readFileSync(path.join(__dirname, 'config.default.json'), 'utf8'));
    return deepMerge(defaults, userConfig || {});
}

function ensureDir(dirPath) {
    if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, { recursive: true });
    }
}

module.exports = {
    id: 'graph',
    name: 'Knowledge Graph',

    register(api) {
        const config = loadConfig(api.pluginConfig || {});
        if (!config.enabled) {
            api.logger.info('Graph plugin disabled via config');
            return;
        }

        const baseDataDir = path.join(__dirname, 'data');
        ensureDir(baseDataDir);
        ensureDir(path.join(baseDataDir, 'agents'));

        // Per-agent state: { store, searcher }
        const states = new Map();

        function getState(agentId) {
            const id = agentId || 'main';
            if (!states.has(id)) {
                // Resolve DB path per agent
                const dbDir = id === 'main'
                    ? baseDataDir
                    : path.join(baseDataDir, 'agents', id);
                ensureDir(dbDir);

                const dbPath = path.join(dbDir, config.storage?.dbFile || 'graph.db');
                const store = new GraphStore(dbPath);
                const searcher = new GraphSearcher(store, config.retrieval);

                states.set(id, { agentId: id, store, searcher });
                api.logger.info(`[Graph] Initialized state for agent "${id}" — db: ${dbPath}`);
            }
            return states.get(id);
        }

        /**
         * Get recent known entities for gazetteer matching.
         * Capped to avoid perf issues on large registries.
         */
        function getKnownEntities(state) {
            try {
                return state.store._recentEntities.all(state.agentId, 200);
            } catch {
                return [];
            }
        }

        // Set up global bus for Phase 2 RRF integration
        if (!global.__ocGraph) {
            global.__ocGraph = { lastResults: {} };
        }

        // -----------------------------------------------------------------
        // HOOK: before_agent_start — Extract entities from query, run search
        // -----------------------------------------------------------------
        // Priority 8: after stability (5) and contemplation (7), before continuity (10).

        api.on('before_agent_start', async (event, ctx) => {
            const state = getState(ctx.agentId);

            // Get user query from event
            const messages = event.messages || [];
            const lastUserMsg = [...messages].reverse().find(m => m.role === 'user');
            if (!lastUserMsg) return {};

            const queryText = extractor.normalizeText(lastUserMsg);
            const stripped = extractor.stripContextBlocks(queryText);
            if (!stripped || stripped.length < 5) return {};

            const knownEntities = getKnownEntities(state);

            // Run link expansion search
            const results = state.searcher.search(stripped, state.agentId, {
                knownEntities
            });

            if (results.exchanges.length === 0) return {};

            // Expose results via global bus for Phase 2 RRF fusion
            global.__ocGraph.lastResults[state.agentId] = {
                exchanges: results.exchanges,
                entities: results.entities,
                timestamp: Date.now()
            };

            api.logger.info(
                `[Graph:${state.agentId}] Found ${results.exchanges.length} connected exchanges via ${results.entities.length} entities`
            );

            return {};
        }, { priority: 8 });

        // -----------------------------------------------------------------
        // HOOK: agent_end — Extract entities + triples from conversation
        // -----------------------------------------------------------------

        api.on('agent_end', async (event, ctx) => {
            // Skip heartbeats
            if (event.metadata?.isHeartbeat) return;
            if (config.extraction?.skipHeartbeats && event.metadata?.isHeartbeat) return;

            const messages = event.messages || [];
            if (messages.length === 0) return;

            // Skip document processing exchanges (same guard as contemplation)
            const firstUserMsg = messages.find(m => m.role === 'user');
            const userText = extractor.normalizeText(firstUserMsg);
            if (/(?:\.pdf|\.docx?|\.txt|\.epub|\.md)\b/i.test(userText) &&
                userText.length > 2000) {
                api.logger.debug(`[Graph:${ctx.agentId}] Skipping document processing exchange`);
                return;
            }

            const state = getState(ctx.agentId);
            const knownEntities = getKnownEntities(state);

            // Extract entities + relationships from the exchange
            const extraction = extractor.extractFromExchange({
                messages,
                config: config.extraction,
                knownEntities
            });

            if (extraction.entities.length === 0) return;

            const sourceExchangeId = event.metadata?.exchangeId
                || event.metadata?.sessionId
                || `exchange_${Date.now()}`;
            const sourceDate = new Date().toISOString().split('T')[0];

            // Write to graph store
            try {
                const tripleIds = state.store.writeExchange({
                    entities: extraction.entities,
                    triples: extraction.triples,
                    cooccurrences: extraction.cooccurrences,
                    agentId: state.agentId,
                    sourceExchangeId,
                    sourceDate
                });

                api.logger.info(
                    `[Graph:${state.agentId}] Extracted ${extraction.entities.length} entities, ` +
                    `wrote ${tripleIds.length} triples from ${sourceExchangeId}`
                );
            } catch (err) {
                api.logger.error(`[Graph:${state.agentId}] Write failed: ${err.message}`);
            }
        });

        // -----------------------------------------------------------------
        // HOOK: session_end — Flush state
        // -----------------------------------------------------------------

        api.on('session_end', async (event, ctx) => {
            // Nothing to flush in Phase 1 — SQLite WAL handles durability.
            // Placeholder for Phase 3 pending resolution queue flush.
            const state = getState(ctx.agentId);
            const stats = state.store.getStats(state.agentId);
            api.logger.info(
                `[Graph:${state.agentId}] Session end — ${stats.entityCount} entities, ${stats.tripleCount} triples`
            );
        });

        // -----------------------------------------------------------------
        // Gateway methods
        // -----------------------------------------------------------------

        api.registerGatewayMethod('graph.getState', async ({ params, respond }) => {
            const state = getState(params?.agentId);
            const stats = state.store.getStats(state.agentId);
            respond(true, {
                agentId: state.agentId,
                entityCount: stats.entityCount,
                tripleCount: stats.tripleCount,
                recentEntities: stats.recentEntities.map(e => ({
                    id: e.id,
                    name: e.canonical_name,
                    type: e.entity_type,
                    mentions: e.mention_count,
                    lastSeen: e.last_seen
                })),
                topCooccurrences: stats.topCooccurrences.map(c => ({
                    entityA: c.entity_a,
                    entityB: c.entity_b,
                    count: c.count,
                    lastSeen: c.last_seen
                }))
            });
        });

        api.registerGatewayMethod('graph.search', async ({ params, respond }) => {
            const state = getState(params?.agentId);
            const knownEntities = getKnownEntities(state);
            const results = state.searcher.search(
                params?.query || '',
                state.agentId,
                { limit: params?.limit, knownEntities }
            );
            respond(true, results);
        });

        api.registerGatewayMethod('graph.getEntity', async ({ params, respond }) => {
            if (!params?.name) {
                respond(false, { error: 'Missing entity name' });
                return;
            }
            const state = getState(params?.agentId);
            const context = state.searcher.getEntityContext(params.name, state.agentId);
            if (!context) {
                respond(false, { error: `Entity not found: ${params.name}` });
                return;
            }
            respond(true, context);
        });

        api.registerGatewayMethod('graph.getTriples', async ({ params, respond }) => {
            const state = getState(params?.agentId);
            const triples = state.store.queryTriples({
                subject: params?.subject,
                predicate: params?.predicate,
                object: params?.object,
                agentId: state.agentId,
                limit: params?.limit
            });
            respond(true, { triples });
        });

        api.registerGatewayMethod('graph.listAgents', async ({ params, respond }) => {
            const agents = [];
            for (const [id, state] of states) {
                const stats = state.store.getStats(id);
                agents.push({
                    agentId: id,
                    entityCount: stats.entityCount,
                    tripleCount: stats.tripleCount
                });
            }
            respond(true, agents);
        });

        api.logger.info('Graph plugin registered — entity extraction + link expansion active');
    }
};
