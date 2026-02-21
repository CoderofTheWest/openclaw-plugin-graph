/**
 * openclaw-plugin-graph — Knowledge Graph Plugin
 *
 * Entity extraction, triple storage, and graph-based retrieval for OpenClaw agents.
 * Phase 1: compromise.js NER + SQLite triples + single-hop link expansion.
 * Phase 3: LLM slow path + conversational entity resolution + metabolism/nightshift.
 * Phase 4: Archive backfill — build graph from historical conversations.
 * Phase 5: Multi-hop traversal + meta-path patterns + graph-aware context injection.
 */

const fs = require('fs');
const path = require('path');
const GraphStore = require('./lib/graph-store');
const GraphSearcher = require('./lib/graph-searcher');
const LLMExtractor = require('./lib/llm-extractor');
const EntityResolver = require('./lib/entity-resolver');
const ArchiveBackfill = require('./lib/backfill');
const ContextBuilder = require('./lib/context-builder');
const PatternDiscovery = require('./lib/pattern-discovery');
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

        // Shared LLM extractor (one instance, stateless)
        const llmExtractor = new LLMExtractor(config.llmExtraction);

        // Per-agent state: { store, searcher, resolver, enrichmentQueue }
        const states = new Map();

        // Resolve continuity plugin data directory (sibling plugin)
        const continuityBaseDir = config.backfill?.continuityDir
            ? path.resolve(__dirname, config.backfill.continuityDir)
            : path.join(__dirname, '..', 'openclaw-plugin-continuity', 'data');

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
                const resolver = new EntityResolver(store, config.entityResolution);

                // Resolve continuity archive dir for this agent
                const archiveDir = id === 'main'
                    ? path.join(continuityBaseDir, 'archive')
                    : path.join(continuityBaseDir, 'agents', id, 'archive');

                const backfill = new ArchiveBackfill({
                    store,
                    extractor,
                    extractionConfig: config.extraction,
                    agentId: id,
                    archiveDir,
                    dataDir: dbDir,
                    logger: api.logger
                });

                // Seed known entities from config (idempotent — upsert won't overwrite)
                const seedEntities = config.entityResolution?.seedEntities || [];
                for (const seed of seedEntities) {
                    if (seed.name) {
                        store.upsertEntity(seed.name, seed.type || 'CONCEPT', id);
                    }
                }
                if (seedEntities.length > 0) {
                    api.logger.info(`[Graph:${id}] Seeded ${seedEntities.length} known entit${seedEntities.length === 1 ? 'y' : 'ies'} from config`);
                }

                // Phase 5: Context builder + pattern discovery
                const contextBuilder = new ContextBuilder(config.contextInjection);
                const patternDiscovery = new PatternDiscovery(store, config.patternDiscovery);

                // Seed static meta-path patterns from config
                if (config.metaPaths?.static) {
                    const seeded = store.seedStaticPatterns(id, config.metaPaths.static);
                    if (seeded > 0) {
                        api.logger.info(`[Graph:${id}] Seeded ${seeded} static meta-path pattern(s)`);
                    }
                }

                states.set(id, {
                    agentId: id,
                    store,
                    searcher,
                    resolver,
                    backfill,
                    contextBuilder,
                    patternDiscovery,
                    enrichmentQueue: [],  // Exchanges queued for LLM slow path
                    isProcessing: false,
                    backfillDone: false,       // Flag: fast-path backfill completed
                    lastPatternDiscovery: 0    // Timestamp of last discovery run
                });
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

            // Phase 4: Lazy archive backfill — run on first conversation
            if (!state.backfillDone) {
                state.backfillDone = true;
                try {
                    const status = state.backfill.checkStatus();
                    if (status.needed) {
                        api.logger.info(
                            `[Graph:${state.agentId}] Starting archive backfill — ` +
                            `${status.unprocessed} unprocessed dates from ${status.available} total`
                        );
                        const result = state.backfill.processAll();
                        api.logger.info(
                            `[Graph:${state.agentId}] Backfill complete — ` +
                            `${result.processed} dates, ${result.entities} entities, ` +
                            `${result.triples} triples in ${result.duration}ms`
                        );
                    }
                } catch (err) {
                    api.logger.warn(`[Graph:${state.agentId}] Backfill failed: ${err.message}`);
                }
            }

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

            if (results.exchanges.length > 0) {
                // Expose results via global bus for Phase 2 RRF fusion
                global.__ocGraph.lastResults[state.agentId] = {
                    exchanges: results.exchanges,
                    entities: results.entities,
                    timestamp: Date.now()
                };

                api.logger.info(
                    `[Graph:${state.agentId}] Found ${results.exchanges.length} connected exchanges via ${results.entities.length} entities`
                );
            }

            // Phase 5: Build graph context summaries
            const contextLines = [];
            if (results.entities.length > 0 && config.contextInjection?.enabled !== false) {
                try {
                    const entityIds = results.entities.map(e => state.store.normalizeEntityId(e.name));
                    const graphContext = state.contextBuilder.buildContext(entityIds, state.agentId, state.store);
                    if (graphContext.length > 0) {
                        contextLines.push('[GRAPH CONTEXT]');
                        contextLines.push('You know these connections:');
                        contextLines.push(...graphContext.map(line => `- ${line}`));
                        api.logger.info(
                            `[Graph:${state.agentId}] Injecting ${graphContext.length} context summary line(s)`
                        );
                    }
                } catch (err) {
                    api.logger.warn(`[Graph:${state.agentId}] Context builder failed: ${err.message}`);
                }
            }

            // Phase 3: Check for ask-tier entity resolution notes
            if (results.entities.length > 0 && config.entityResolution?.method !== 'exact') {
                try {
                    const askNotes = state.resolver.getAskNotes(results.entities, state.agentId);
                    if (askNotes.length > 0) {
                        contextLines.push('[GRAPH NOTE]');
                        contextLines.push(...askNotes);
                        api.logger.info(
                            `[Graph:${state.agentId}] Injecting ${askNotes.length} entity resolution note(s)`
                        );
                    }
                } catch (err) {
                    api.logger.warn(`[Graph:${state.agentId}] Entity resolution check failed: ${err.message}`);
                }
            }

            if (contextLines.length > 0) {
                return { prependContext: contextLines.join('\n') };
            }
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

            // Resolve extracted entities to canonical names (assume-tier only)
            if (config.entityResolution?.method !== 'exact') {
                const contextNames = extraction.entities.map(e => e.name);
                for (const entity of extraction.entities) {
                    try {
                        const resolved = state.resolver.resolve(entity.name, state.agentId, contextNames);
                        if (resolved.tier === 'exact' && resolved.entity) {
                            entity.name = resolved.entity.canonical_name;
                        } else if (resolved.tier === 'assume' && resolved.entity) {
                            entity.name = resolved.entity.canonical_name;
                        }
                    } catch { /* resolution failure is non-fatal */ }
                }
                // Also canonicalize triple subjects/objects
                for (const triple of extraction.triples) {
                    try {
                        const subResolved = state.resolver.resolve(triple.subject, state.agentId, contextNames);
                        if ((subResolved.tier === 'exact' || subResolved.tier === 'assume') && subResolved.entity) {
                            triple.subject = subResolved.entity.canonical_name;
                        }
                        const objResolved = state.resolver.resolve(triple.object, state.agentId, contextNames);
                        if ((objResolved.tier === 'exact' || objResolved.tier === 'assume') && objResolved.entity) {
                            triple.object = objResolved.entity.canonical_name;
                        }
                    } catch { /* resolution failure is non-fatal */ }
                }
            }

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

                // Phase 3: Queue for LLM slow-path enrichment (capped at 50 pending)
                if (state.enrichmentQueue.length < 50) {
                    const lastAgent = [...messages].reverse().find(m => m.role === 'assistant');
                    state.enrichmentQueue.push({
                        exchangeId: sourceExchangeId,
                        userText: extractor.stripContextBlocks(userText),
                        agentText: extractor.normalizeText(lastAgent),
                        date: sourceDate,
                        queuedAt: Date.now()
                    });
                }
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
        // Phase 3: LLM enrichment processor
        // -----------------------------------------------------------------

        /**
         * Process queued exchanges through LLM extractor.
         * Called by nightshift during idle time.
         */
        async function processEnrichmentQueue(state, maxItems) {
            if (state.isProcessing || state.enrichmentQueue.length === 0) return 0;
            state.isProcessing = true;

            const batch = state.enrichmentQueue.splice(0, maxItems || 3);
            let processed = 0;

            for (const item of batch) {
                try {
                    const result = await llmExtractor.extract(item.userText, item.agentText);
                    if (result.entities.length === 0 && result.relationships.length === 0) continue;

                    // Write LLM-extracted entities with alias tracking
                    for (const entity of result.entities) {
                        const id = state.store.upsertEntity(entity.name, entity.type, state.agentId);
                        // Store aliases if provided
                        if (entity.aliases && entity.aliases.length > 0) {
                            const existing = state.store.getEntity(id);
                            if (existing) {
                                let currentAliases = [];
                                try { currentAliases = JSON.parse(existing.aliases || '[]'); } catch { /* */ }
                                const merged = [...new Set([...currentAliases, ...entity.aliases])];
                                state.store.db.prepare(
                                    'UPDATE entities SET aliases = ? WHERE id = ?'
                                ).run(JSON.stringify(merged), id);
                            }
                        }
                    }

                    // Write LLM-extracted relationships (higher confidence than fast path)
                    for (const rel of result.relationships) {
                        // Use entity resolver for subject and object
                        const contextNames = result.entities.map(e => e.name);
                        const subjectRes = state.resolver.resolve(rel.subject, state.agentId, contextNames);
                        const objectRes = state.resolver.resolve(rel.object, state.agentId, contextNames);

                        // Determine pending status
                        const pending = subjectRes.tier === 'defer' || objectRes.tier === 'defer';

                        state.store.addTriple({
                            subject: rel.subject,
                            predicate: rel.predicate,
                            object: rel.object,
                            confidence: Math.min(rel.confidence + 0.1, 1.0), // LLM boost
                            sourceExchangeId: item.exchangeId,
                            sourceDate: item.date,
                            agentId: state.agentId,
                            pendingResolution: pending
                        });
                    }

                    processed++;
                    api.logger.info(
                        `[Graph:${state.agentId}] LLM enriched ${item.exchangeId}: ` +
                        `${result.entities.length} entities, ${result.relationships.length} relationships`
                    );
                } catch (err) {
                    api.logger.warn(
                        `[Graph:${state.agentId}] LLM enrichment failed for ${item.exchangeId}: ${err.message}`
                    );
                }
            }

            state.isProcessing = false;
            return processed;
        }

        // -----------------------------------------------------------------
        // Phase 3: Metabolism + Nightshift integration
        // Deferred to next tick because graph loads before metabolism/nightshift.
        // -----------------------------------------------------------------

        setImmediate(() => {
            // Metabolism subscription — listen for knowledge gaps
            if (global.__ocMetabolism?.gapListeners) {
                global.__ocMetabolism.gapListeners.push((gaps, agentId) => {
                    api.logger.debug(
                        `[Graph:${agentId}] Received ${gaps.length} gap(s) from metabolism`
                    );
                });
                api.logger.info('[Graph] Subscribed to metabolism gap events');
            }

            // Nightshift task registration — LLM enrichment + resolution
            if (global.__ocNightshift?.registerTaskRunner) {
            global.__ocNightshift.registerTaskRunner('graph-enrichment', async (task, ctx) => {
                const state = getState(ctx.agentId);

                // 0. Phase 4: Incremental backfill — check for new archive dates
                try {
                    const backfillResult = state.backfill.processBatch(5);
                    if (backfillResult.processed > 0) {
                        api.logger.info(
                            `[Graph:${state.agentId}] Nightshift backfill: ${backfillResult.processed} dates, ` +
                            `${backfillResult.triples} triples, ${backfillResult.remaining} remaining`
                        );
                    }
                } catch (err) {
                    api.logger.warn(`[Graph:${state.agentId}] Nightshift backfill error: ${err.message}`);
                }

                // 1. Process queued exchanges through LLM extractor
                const enriched = await processEnrichmentQueue(state, 3);

                // 2. Process pending entity resolutions
                const resolved = state.resolver.processPending(state.agentId);

                // 3. Detect graph-based knowledge gaps → feed to contemplation
                const gaps = detectGraphGaps(state);
                if (gaps.length > 0 && global.__ocMetabolism?.gapListeners) {
                    // Push graph gaps through the same pipeline contemplation uses
                    for (const listener of global.__ocMetabolism.gapListeners) {
                        try {
                            listener(gaps, state.agentId);
                        } catch (e) {
                            api.logger.warn(`[Graph:${state.agentId}] Gap listener error: ${e.message}`);
                        }
                    }
                    api.logger.info(
                        `[Graph:${state.agentId}] Emitted ${gaps.length} graph gap(s) to contemplation`
                    );
                }

                // 4. Phase 5: Pattern discovery + validation (gated to once per 24h)
                const discoveryInterval = (config.patternDiscovery?.discoveryIntervalHours || 24) * 3600000;
                if (config.patternDiscovery?.enabled !== false &&
                    Date.now() - state.lastPatternDiscovery > discoveryInterval) {
                    try {
                        state.lastPatternDiscovery = Date.now();

                        // Decay stale triples before pattern evaluation
                        const decayed = state.store.decayStaleTriples(state.agentId,
                            config.storage?.confidenceHalfLifeDays || 90);
                        if (decayed.changes > 0) {
                            api.logger.info(
                                `[Graph:${state.agentId}] Decayed confidence on ${decayed.changes} stale triple(s)`
                            );
                        }

                        const discovered = state.patternDiscovery.discover(state.agentId);
                        const validated = state.patternDiscovery.validatePatterns(state.agentId);
                        if (discovered.saved > 0 || validated.retired > 0) {
                            api.logger.info(
                                `[Graph:${state.agentId}] Pattern discovery: ` +
                                `${discovered.candidates} candidates → ${discovered.novel} novel → ${discovered.saved} saved. ` +
                                `Validated: ${validated.validated} ok, ${validated.retired} retired`
                            );
                        }
                    } catch (err) {
                        api.logger.warn(`[Graph:${state.agentId}] Pattern discovery error: ${err.message}`);
                    }
                }

                api.logger.info(
                    `[Graph:${state.agentId}] Nightshift: enriched ${enriched} exchanges, ` +
                    `resolved ${resolved.resolved} pending, expired ${resolved.expired}`
                );

                // Re-queue if there's more work
                const hasMore = state.enrichmentQueue.length > 0
                    || state.backfill.getUnprocessedDates().length > 0;
                if (hasMore) {
                    global.__ocNightshift.queueTask(state.agentId, {
                        type: 'graph-enrichment',
                        priority: 35
                    });
                }
            });
            api.logger.info('[Graph] Registered nightshift task runner for "graph-enrichment"');
        }
        }); // end setImmediate

        // -----------------------------------------------------------------
        // Phase 3: Queue nightshift tasks on heartbeat if work is pending
        // -----------------------------------------------------------------

        api.on('heartbeat', async (event, ctx) => {
            const state = getState(ctx.agentId);
            if (state.enrichmentQueue.length > 0 && global.__ocNightshift?.queueTask) {
                global.__ocNightshift.queueTask(state.agentId, {
                    type: 'graph-enrichment',
                    priority: 35  // Between contemplation (50) and metabolism (10)
                });
            }
        });

        // -----------------------------------------------------------------
        // Phase 3: Contemplation gap detection — graph-derived knowledge gaps
        // -----------------------------------------------------------------

        /**
         * Detect knowledge gaps from graph structure.
         * Called periodically or on demand via gateway method.
         *
         * Gap types:
         * - Entities with high mentions but few relationships
         * - Entities connected only by related_to (no specific predicates)
         * - Temporal dead zones (entity not mentioned in 30+ days)
         */
        function detectGraphGaps(state) {
            const gaps = [];
            const agentId = state.agentId;

            try {
                // 1. Under-connected entities: high mentions, few triples
                const popular = state.store.db.prepare(`
                    SELECT e.id, e.canonical_name, e.entity_type, e.mention_count,
                           COUNT(t.id) as triple_count
                    FROM entities e
                    LEFT JOIN triples t ON (t.subject = e.id OR t.object = e.id) AND t.agent_id = ?
                    WHERE e.agent_id = ? AND e.mention_count > 5
                    GROUP BY e.id
                    HAVING triple_count < 3
                    ORDER BY e.mention_count DESC
                    LIMIT 5
                `).all(agentId, agentId);

                for (const e of popular) {
                    gaps.push({
                        question: `What do we actually know about ${e.canonical_name}? It's been mentioned ${e.mention_count} times but has very few recorded relationships.`,
                        type: 'under_connected',
                        sourceId: `graph:${e.id}`
                    });
                }

                // 2. Generic-only relationships: only related_to, no specific predicates
                const genericOnly = state.store.db.prepare(`
                    SELECT e.id, e.canonical_name, e.mention_count
                    FROM entities e
                    WHERE e.agent_id = ?
                      AND e.mention_count > 3
                      AND NOT EXISTS (
                          SELECT 1 FROM triples t
                          WHERE (t.subject = e.id OR t.object = e.id)
                            AND t.agent_id = ?
                            AND t.predicate != 'related_to'
                      )
                      AND EXISTS (
                          SELECT 1 FROM triples t
                          WHERE (t.subject = e.id OR t.object = e.id)
                            AND t.agent_id = ?
                      )
                    LIMIT 5
                `).all(agentId, agentId, agentId);

                for (const e of genericOnly) {
                    gaps.push({
                        question: `How specifically is ${e.canonical_name} connected to other things we know about? We only have generic associations so far.`,
                        type: 'generic_only',
                        sourceId: `graph:${e.id}`
                    });
                }

                // 3. Temporal dead zones: entities not seen in 30+ days that were previously active
                const staleDate = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
                const stale = state.store.db.prepare(`
                    SELECT id, canonical_name, mention_count, last_seen
                    FROM entities
                    WHERE agent_id = ? AND mention_count > 5
                      AND last_seen < ?
                    ORDER BY mention_count DESC
                    LIMIT 3
                `).all(agentId, staleDate);

                for (const e of stale) {
                    gaps.push({
                        question: `We haven't discussed ${e.canonical_name} in over 30 days — has anything changed?`,
                        type: 'temporal_dead_zone',
                        sourceId: `graph:${e.id}`
                    });
                }
            } catch (err) {
                api.logger.warn(`[Graph:${agentId}] Gap detection error: ${err.message}`);
            }

            return gaps;
        }

        // Feed graph gaps into contemplation's inquiry queue (if available)
        // This runs during nightshift graph-enrichment task
        if (global.__ocMetabolism?.gapListeners) {
            // We don't emit through metabolism — instead we expose a method
            // that contemplation can call, or we push during nightshift.
            // The nightshift task already handles this above.
        }

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

        api.registerGatewayMethod('graph.detectGaps', async ({ params, respond }) => {
            const state = getState(params?.agentId);
            const gaps = detectGraphGaps(state);
            respond(true, { gaps });
        });

        api.registerGatewayMethod('graph.resolveEntity', async ({ params, respond }) => {
            if (!params?.name) {
                respond(false, { error: 'Missing entity name' });
                return;
            }
            const state = getState(params?.agentId);
            const contextEntities = params?.context || [];
            const result = state.resolver.resolve(params.name, state.agentId, contextEntities);
            respond(true, result);
        });

        api.registerGatewayMethod('graph.mergeEntities', async ({ params, respond }) => {
            if (!params?.keepId || !params?.mergeId) {
                respond(false, { error: 'Missing keepId or mergeId' });
                return;
            }
            const state = getState(params?.agentId);
            const result = state.resolver.mergeEntities(
                params.keepId, params.mergeId, state.agentId
            );
            respond(true, result);
        });

        api.registerGatewayMethod('graph.backfillStatus', async ({ params, respond }) => {
            const state = getState(params?.agentId);
            const status = state.backfill.checkStatus();
            const processed = state.backfill.getProcessedDates();
            respond(true, {
                agentId: state.agentId,
                ...status,
                processedDates: processed.size,
                backfillDone: state.backfillDone
            });
        });

        api.registerGatewayMethod('graph.rebuild', async ({ params, respond }) => {
            const state = getState(params?.agentId);
            api.logger.info(`[Graph:${state.agentId}] Rebuild requested — clearing graph and re-extracting`);

            // 1. Reset graph and backfill log
            const cleared = state.backfill.reset();
            api.logger.info(
                `[Graph:${state.agentId}] Cleared ${cleared.triplesDeleted} triples, ${cleared.entitiesDeleted} entities`
            );

            // 2. Re-run fast-path backfill
            const result = state.backfill.processAll();
            api.logger.info(
                `[Graph:${state.agentId}] Rebuild complete — ${result.processed} dates, ` +
                `${result.entities} entities, ${result.triples} triples in ${result.duration}ms`
            );

            respond(true, {
                agentId: state.agentId,
                cleared,
                rebuilt: result
            });
        });

        api.registerGatewayMethod('graph.getPatterns', async ({ params, respond }) => {
            const state = getState(params?.agentId);
            const patterns = state.store.getActivePatterns(state.agentId);
            respond(true, { agentId: state.agentId, patterns });
        });

        api.registerGatewayMethod('graph.discoverPatterns', async ({ params, respond }) => {
            const state = getState(params?.agentId);
            const discovered = state.patternDiscovery.discover(state.agentId);
            const validated = state.patternDiscovery.validatePatterns(state.agentId);
            respond(true, { agentId: state.agentId, discovered, validated });
        });

        api.logger.info('Graph plugin registered — Phases 1-5: extraction + multi-hop traversal + meta-paths + context injection active');
    }
};
