/**
 * Entity Extractor — compromise.js NER + regex fallback + heuristic relationships.
 *
 * Phase 1: Fast path only. Sub-millisecond per exchange.
 * Phase 3 adds LLM slow path (lib/llm-extractor.js) for higher quality.
 *
 * Returns { entities, triples, cooccurrences } ready for GraphStore.writeExchange().
 */

const nlp = require('compromise');

// ── Regex fallback patterns (things compromise misses) ──────────────────────

const MENTION_PATTERN = /@(\w{2,30})/g;
const EMAIL_PATTERN = /[\w.+-]+@[\w-]+\.[\w.-]+/g;
const URL_PATTERN = /https?:\/\/[^\s<>)"']+/g;

// ── Relationship patterns → canonical predicates ────────────────────────────
// Order matters: first match wins. Keep the most specific patterns first.

// Subject: capitalized word(s). Object: 1-4 words, stopping at prepositions/conjunctions/punctuation.
// The object group uses a lookahead-free approach: capture non-stop words.
const RELATIONSHIP_PATTERNS = [
    // "X created Y", "X built Y", "X made Y", "X wrote Y"
    { pattern: /(\b[A-Z][\w]*(?:\s+[A-Z][\w]*)*)\s+(?:created|built|made|wrote|designed|developed)\s+([\w][\w-]*(?:\s+[\w][\w-]*){0,3})(?=\s+(?:in|at|on|for|with|from|to|and|but|or)\b|\.|,|;|$)/gi, predicate: 'created' },
    // "X uses Y", "X is using Y", "X runs on Y"
    { pattern: /(\b[A-Z][\w]*(?:\s+[A-Z][\w]*)*)\s+(?:uses?|is using|runs? on|runs? with)\s+([\w][\w-]*(?:\s+[\w][\w-]*){0,3})(?=\s+(?:in|at|on|for|with|from|to|and|but|or)\b|\.|,|;|$)/gi, predicate: 'uses' },
    // "X works on Y", "X is working on Y"
    { pattern: /(\b[A-Z][\w]*(?:\s+[A-Z][\w]*)*)\s+(?:works? on|is working on|working on)\s+([\w][\w-]*(?:\s+[\w][\w-]*){0,3})(?=\s+(?:in|at|on|for|with|from|to|and|but|or)\b|\.|,|;|$)/gi, predicate: 'works_on' },
    // "X knows Y", "X met Y"
    { pattern: /(\b[A-Z][\w]*(?:\s+[A-Z][\w]*)*)\s+(?:knows?|met|talked to|spoke with)\s+([\w][\w-]*(?:\s+[\w][\w-]*){0,3})(?=\s+(?:in|at|on|for|with|from|to|and|but|or)\b|\.|,|;|$)/gi, predicate: 'knows' },
    // "X lives in Y", "X is in Y", "X is from Y"
    { pattern: /(\b[A-Z][\w]*(?:\s+[A-Z][\w]*)*)\s+(?:lives? in|is in|is from|located in|based in|moved to)\s+([\w][\w-]*(?:\s+[\w][\w-]*){0,3})(?=\s+(?:and|but|or|because|since|after|before)\b|\.|,|;|$)/gi, predicate: 'located_in' },
    // "X is part of Y", "X belongs to Y"
    { pattern: /(\b[A-Z][\w]*(?:\s+[A-Z][\w]*)*)\s+(?:is part of|belongs? to|is a component of|is inside)\s+([\w][\w-]*(?:\s+[\w][\w-]*){0,3})(?=\s+(?:in|at|on|for|with|from|to|and|but|or)\b|\.|,|;|$)/gi, predicate: 'part_of' },
    // "X is interested in Y", "X likes Y", "X loves Y"
    { pattern: /(\b[A-Z][\w]*(?:\s+[A-Z][\w]*)*)\s+(?:is interested in|likes?|loves?|enjoys?|is into)\s+([\w][\w-]*(?:\s+[\w][\w-]*){0,3})(?=\s+(?:in|at|on|for|with|from|to|and|but|or)\b|\.|,|;|$)/gi, predicate: 'interested_in' },
    // "X prefers Y", "X would rather Y"
    { pattern: /(\b[A-Z][\w]*(?:\s+[A-Z][\w]*)*)\s+(?:prefers?|would rather|favou?rs?)\s+([\w][\w-]*(?:\s+[\w][\w-]*){0,3})(?=\s+(?:in|at|on|for|with|from|to|and|but|or)\b|\.|,|;|$)/gi, predicate: 'prefers' },
];

// ── Context block patterns to strip (same as contemplation) ─────────────────

const CONTEXT_BLOCK_PATTERN = /\[(?:CONTINUITY CONTEXT|STABILITY CONTEXT|GROWTH VECTORS|MEMORY INTEGRATION|CONTEMPLATION STATE|GRAPH CONTEXT)\][\s\S]*?(?=\[(?:CONTINUITY|STABILITY|GROWTH|MEMORY|CONTEMPLATION|GRAPH)|$)/gi;
const TIMESTAMP_PREFIX = /^\[(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) \d{4}-\d{2}-\d{2} \d{2}:\d{2} [A-Z]+\]\s*/;

// ── Noise words to skip as entities ─────────────────────────────────────────

const NOISE_ENTITIES = new Set([
    'i', 'me', 'my', 'we', 'us', 'you', 'your', 'he', 'she', 'they', 'it',
    'this', 'that', 'here', 'there', 'now', 'then', 'today', 'tomorrow',
    'yes', 'no', 'okay', 'sure', 'thanks', 'thank', 'please', 'sorry',
    'hey', 'hi', 'hello', 'hmm', 'huh', 'oh', 'ah', 'um', 'uh',
    'the', 'a', 'an', 'some', 'any', 'all', 'each', 'every',
    'just', 'really', 'very', 'also', 'too', 'well', 'still',
]);

/**
 * Normalize message content to plain text string.
 */
function normalizeText(msg) {
    if (!msg) return '';
    if (typeof msg === 'string') return msg;
    if (typeof msg.content === 'string') return msg.content;
    if (Array.isArray(msg.content)) {
        return msg.content.map(part => part?.text || part?.content || '').join(' ');
    }
    return String(msg.content || '');
}

/**
 * Strip injected context blocks from message text.
 */
function stripContextBlocks(text) {
    let cleaned = text.replace(CONTEXT_BLOCK_PATTERN, '');
    cleaned = cleaned.replace(TIMESTAMP_PREFIX, '');
    return cleaned.trim();
}

/**
 * Extract entities from text using compromise.js + regex fallbacks.
 *
 * Returns array of { name, type } objects.
 */
function extractEntities(text, config) {
    if (!text || text.length < (config?.minExchangeLength || 20)) return [];

    const entities = [];
    const seen = new Set();

    function addEntity(name, type) {
        // Strip trailing punctuation that compromise sometimes includes
        const trimmed = name.trim().replace(/[.,;:!?]+$/, '').trim();
        if (trimmed.length < 2) return;
        if (NOISE_ENTITIES.has(trimmed.toLowerCase())) return;
        const key = trimmed.toLowerCase();
        if (seen.has(key)) return;
        seen.add(key);
        entities.push({ name: trimmed, type });
    }

    // ── compromise.js NER ───────────────────────────────────────────────
    const doc = nlp(text);

    for (const person of doc.people().out('array')) {
        addEntity(person, 'PERSON');
    }

    for (const place of doc.places().out('array')) {
        addEntity(place, 'PLACE');
    }

    for (const org of doc.organizations().out('array')) {
        addEntity(org, 'ORGANIZATION');
    }

    // compromise v14 base doesn't have .dates() — use regex for dates
    // Match ISO dates, month names + year, and common date formats
    const datePatterns = /\b(\d{4}-\d{2}-\d{2}|\d{1,2}\/\d{1,2}\/\d{2,4}|(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2}(?:,?\s*\d{4})?)\b/gi;
    let dateMatch;
    while ((dateMatch = datePatterns.exec(text)) !== null) {
        addEntity(dateMatch[0], 'DATE');
    }

    // ── Regex fallbacks ─────────────────────────────────────────────────

    // @mentions
    let match;
    MENTION_PATTERN.lastIndex = 0;
    while ((match = MENTION_PATTERN.exec(text)) !== null) {
        addEntity(match[1], 'PERSON');
    }

    // Email addresses → organization/contact
    EMAIL_PATTERN.lastIndex = 0;
    while ((match = EMAIL_PATTERN.exec(text)) !== null) {
        addEntity(match[0], 'THING');
    }

    // URLs → thing
    URL_PATTERN.lastIndex = 0;
    while ((match = URL_PATTERN.exec(text)) !== null) {
        addEntity(match[0], 'THING');
    }

    // Cap entities per exchange
    const maxEntities = config?.maxEntitiesPerExchange || 15;
    return entities.slice(0, maxEntities);
}

/**
 * Gazetteer matching — find known entities from the registry in the text.
 * Takes a list of known entity names and checks if any appear in the text.
 *
 * Returns array of { name, type } for matches found.
 */
function matchGazetteer(text, knownEntities) {
    if (!text || !knownEntities || knownEntities.length === 0) return [];

    const lowered = text.toLowerCase();
    const matches = [];

    for (const entity of knownEntities) {
        // Only match entities with 3+ chars to avoid false positives
        if (entity.canonical_name.length < 3) continue;
        if (lowered.includes(entity.canonical_name.toLowerCase())) {
            matches.push({ name: entity.canonical_name, type: entity.entity_type });
        }
    }

    return matches;
}

/**
 * Trim trailing stop words (prepositions, conjunctions, articles) from a captured phrase.
 * "OpenClaw in Montana" → "OpenClaw", "GLM-5 for both" → "GLM-5"
 */
const STOP_WORDS = new Set([
    'in', 'at', 'on', 'for', 'with', 'from', 'to', 'and', 'but', 'or',
    'the', 'a', 'an', 'of', 'by', 'about', 'both', 'also', 'too',
    'is', 'are', 'was', 'were', 'be', 'been', 'being', 'has', 'have', 'had',
    'that', 'which', 'who', 'where', 'when', 'while', 'as', 'so', 'if'
]);

function trimStopWords(phrase) {
    // Split at interior prepositions/conjunctions: "OpenClaw in Montana" → "OpenClaw"
    let cleaned = phrase.split(/\s+(?:in|at|on|for|with|from|to|and|but|or|about|by|both)\s+/i)[0];
    // Also trim trailing stop words
    const words = cleaned.split(/\s+/);
    while (words.length > 1 && STOP_WORDS.has(words[words.length - 1].toLowerCase())) {
        words.pop();
    }
    return words.join(' ').replace(/[.,;:!?]+$/, '');
}

/**
 * Extract explicit relationships from text using pattern matching.
 *
 * Returns array of { subject, predicate, object, confidence } triples.
 */
function extractRelationships(text, canonicalPredicates) {
    if (!text) return [];

    const triples = [];
    const seen = new Set();
    const validPredicates = new Set(canonicalPredicates || []);

    for (const { pattern, predicate } of RELATIONSHIP_PATTERNS) {
        // Only extract relationships using canonical predicates
        if (validPredicates.size > 0 && !validPredicates.has(predicate)) continue;

        pattern.lastIndex = 0;
        let match;
        while ((match = pattern.exec(text)) !== null) {
            const subject = match[1].trim();
            // Trim trailing stop words from object (prepositions, conjunctions, articles)
            const object = trimStopWords(match[2].trim());

            if (subject.length < 2 || object.length < 2) continue;
            if (NOISE_ENTITIES.has(subject.toLowerCase())) continue;
            if (NOISE_ENTITIES.has(object.toLowerCase())) continue;

            const key = `${subject.toLowerCase()}|${predicate}|${object.toLowerCase()}`;
            if (seen.has(key)) continue;
            seen.add(key);

            triples.push({
                subject,
                predicate,
                object,
                confidence: 0.7 // pattern-matched = moderate confidence
            });
        }
    }

    return triples;
}

/**
 * Generate co-occurrence pairs from a list of entities.
 * All entities seen in the same exchange are considered co-occurring.
 *
 * Returns array of [entityNameA, entityNameB] pairs.
 */
function generateCooccurrences(entities) {
    if (!entities || entities.length < 2) return [];

    const pairs = [];
    for (let i = 0; i < entities.length; i++) {
        for (let j = i + 1; j < entities.length; j++) {
            pairs.push([entities[i].name, entities[j].name]);
        }
    }
    return pairs;
}

/**
 * Full extraction pipeline for an exchange.
 *
 * Takes messages (user + agent) and returns everything needed
 * for GraphStore.writeExchange():
 *   { entities, triples, cooccurrences }
 *
 * @param {Object} params
 * @param {Array} params.messages - Array of message objects ({ role, content })
 * @param {Object} params.config - Extraction config from plugin config
 * @param {Array} params.knownEntities - Known entities from registry (for gazetteer)
 * @returns {{ entities: Array, triples: Array, cooccurrences: Array }}
 */
function extractFromExchange({ messages, config, knownEntities }) {
    const cfg = config || {};

    // Collect + clean text from messages
    const textParts = [];
    for (const msg of (messages || [])) {
        const raw = normalizeText(msg);
        if (!raw) continue;
        const cleaned = stripContextBlocks(raw);
        if (cleaned.length > 10) {
            textParts.push(cleaned);
        }
    }

    const fullText = textParts.join('\n\n');
    if (fullText.length < (cfg.minExchangeLength || 20)) {
        return { entities: [], triples: [], cooccurrences: [] };
    }

    // 1. Extract entities
    const entities = extractEntities(fullText, cfg);

    // 2. Gazetteer matching — find known entities that compromise missed
    const gazetteered = matchGazetteer(fullText, knownEntities || []);
    const entityNames = new Set(entities.map(e => e.name.toLowerCase()));
    for (const g of gazetteered) {
        if (!entityNames.has(g.name.toLowerCase())) {
            entities.push(g);
            entityNames.add(g.name.toLowerCase());
        }
    }

    // 3. Extract explicit relationships
    const explicitTriples = extractRelationships(fullText, cfg.canonicalPredicates);

    // 4. Generate co-occurrence-based triples (all entities → related_to)
    const cooccurrences = generateCooccurrences(entities);

    // 5. Build final triples list (explicit + co-occurrence related_to)
    const triples = [...explicitTriples];
    const tripleKeys = new Set(explicitTriples.map(t =>
        `${t.subject.toLowerCase()}|${t.object.toLowerCase()}`
    ));

    // Add related_to triples for co-occurring entities that don't have
    // an explicit relationship already
    for (const [a, b] of cooccurrences) {
        const keyAB = `${a.toLowerCase()}|${b.toLowerCase()}`;
        const keyBA = `${b.toLowerCase()}|${a.toLowerCase()}`;
        if (!tripleKeys.has(keyAB) && !tripleKeys.has(keyBA)) {
            triples.push({
                subject: a,
                predicate: 'related_to',
                object: b,
                confidence: 0.4 // co-occurrence = low confidence
            });
        }
    }

    // Filter by confidence threshold
    const threshold = cfg.confidenceThreshold || 0.5;
    const filteredTriples = triples.filter(t => t.confidence >= threshold);

    return {
        entities,
        triples: filteredTriples,
        cooccurrences
    };
}

module.exports = {
    extractEntities,
    extractRelationships,
    matchGazetteer,
    generateCooccurrences,
    extractFromExchange,
    normalizeText,
    stripContextBlocks
};
