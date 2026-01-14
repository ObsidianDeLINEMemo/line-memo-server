export interface Env {
    LINE_CHANNEL_SECRET: string;
    API_TOKEN: string; // The token for Obsidian to authenticate
    LINE_MEMO_KV: KVNamespace;
}

interface LineWebhookEvent {
    type: string;
    message: {
        type: string;
        id: string;
        text: string;
    };
    timestamp: number;
    source: {
        type: string;
        userId: string;
    };
    replyToken: string;
}

export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        const url = new URL(request.url);

        // 1. Webhook Endpoint
        if (request.method === 'POST' && url.pathname === '/line/webhook') {
            return handleWebhook(request, env);
        }

        // 2. Pull Endpoint
        if (request.method === 'GET' && url.pathname === '/api/pull') {
            return handlePull(request, env);
        }

        // 3. Ack Endpoint
        if (request.method === 'POST' && url.pathname === '/api/ack') {
            return handleAck(request, env);
        }

        return new Response('Not Found', { status: 404 });
    },
};

// --- Webhook ---

async function handleWebhook(request: Request, env: Env): Promise<Response> {
    // Verify Signature
    const signature = request.headers.get('x-line-signature');
    const body = await request.text();

    if (!signature || !await verifySignature(env.LINE_CHANNEL_SECRET, signature, body)) {
        return new Response('Invalid signature', { status: 401 });
    }

    const json = JSON.parse(body);
    const events: LineWebhookEvent[] = json.events;

    for (const event of events) {
        if (event.type === 'message' && event.message.type === 'text') {
            // Store in KV
            const key = `msg:${event.timestamp}:${event.message.id}`;
            const data = {
                messageId: event.message.id,
                userId: event.source.userId,
                text: event.message.text,
                receivedAt: event.timestamp,
                createdAt: Date.now()
            };
            // Store with expiration (10 days = 864000s)
            await env.LINE_MEMO_KV.put(key, JSON.stringify(data), { expirationTtl: 864000 });
        }
    }

    return new Response('OK');
}

// --- Pull ---

async function handlePull(request: Request, env: Env): Promise<Response> {
    if (!checkAuth(request, env)) return new Response('Unauthorized', { status: 401 });

    // List keys
    // Prefix-based listing. We just get the list.
    // limit defaults to 50
    const url = new URL(request.url);
    const limit = parseInt(url.searchParams.get('limit') || '50');

    const list = await env.LINE_MEMO_KV.list({ prefix: 'msg:', limit: limit });
    const messages = [];

    for (const key of list.keys) {
        const val = await env.LINE_MEMO_KV.get(key.name);
        if (val) {
            messages.push(JSON.parse(val));
        }
    }

    return new Response(JSON.stringify({ messages }), {
        headers: { 'Content-Type': 'application/json' }
    });
}

// --- Ack ---

async function handleAck(request: Request, env: Env): Promise<Response> {
    if (!checkAuth(request, env)) return new Response('Unauthorized', { status: 401 });

    const body: any = await request.json();
    const messageIds = body.messageIds; // Array of strings

    if (!Array.isArray(messageIds)) {
        return new Response('Invalid body', { status: 400 });
    }

    let deletedCount = 0;
    // We need to find keys for these IDs.
    // Since KV doesn't support "get key by value", and our key contains timestamp,
    // checking efficiently is tricky if we don't know the timestamp.
    // However, the Plugin just received the message, so it has the full object including timestamp?
    // The Plugin sent back `messageIds`.
    // Issue: We can't derive the KV Key from just `messageId` if the Key has timestamp.
    // FIX: We should either:
    // A) Have the plugin send back the KV Key (opaque token).
    // B) Store the Key in the message body sent to plugin.

    // Let's rely on the fact that we list keys on Pull.
    // We can iterate again? No, expensive.
    // Better: The `pull` response should include the kvKey? Or implicit?
    // Let's change `pull` to return the `key` as well, or `ack` to accept `messageId` and we scan?
    // Scanning is bad.

    // Simplest: The Plugin's `messageId` is used for dedupe, but when Acking, the Plugin should send what it got.
    // Let's assume the Plugin sends `messageId`.
    // We can change the KV Key structure to `msg:${messageId}` to make it O(1)?
    // Timestamp in Key was for ordering? `list` returns in lexicographical order.
    // `msg:${messageId}` is random order.
    // We want FIFO usually.
    // `msg:${timestamp}:${messageId}` is good for FIFO.

    // Solution: When we PULL, we give the plugin the data. 
    // The plugin handles "messageId" for its business logic. 
    // But for ACK, maybe we should just accept the list of Keys to delete?
    // But security risk? If client sends arbitrary keys?
    // Client is trusted (Auth).
    // So, let's include `kvKey` in the Pull response.

    // Refactor Pull:
    // messages: [{ ...data, kvKey: "..." }]

    // Refactor Ack:
    // body: { kvKeys: [...] }

    // Wait, the User Requirement says "ack API for processed messageId".
    // "POST /api/ack (取得済messageIdを処理済みに)"
    // Implementation detail can vary as long as the interface works.
    // I will include `key` in the pull response but mapped to `id` for internal use?
    // No, the prompt spec is strict about "messageId, userId...". It doesn't mention `kvKey`.
    // But I can add extra fields.

    // Re-reading code:
    // handlePull gets value.
    // messageId is inside value.
    // I will change KV Key to just `msg:${messageId}`?
    // Then order is lost.
    // But `list` returns keys.
    // If I use `msg:${timestamp}:${messageId}`, I can't delete by ID easily without timestamp.

    // Compromise: Use `msg:${timestamp}:${messageId}`.
    // When PULL, return list.
    // When ACK, iterating the current LIST of keys to find matches is slow?
    // No, listing 50 keys is fast.
    // If the backlog is huge, this is slow.

    // Alternate: Store a secondary index? No.
    // Alternate: Just expect the Client to send the `receivedAt` (timestamp) back in the ACK?
    // The Plugin has `receivedAt`.
    // So Ack body: `[{ messageId: "...", receivedAt: 12345 }]`.
    // Then we reconstruct the key: `msg:${receivedAt}:${messageId}`.
    // This works!

    // I'll update the ACK logic to expect `messages: { messageId: string, receivedAt: number }[]` or simliar.
    // But the plugin implementation I wrote above sends `messageIds: string[]`.
    // I should update the Plugin code or the Worker code.
    // Updating Plugin is easier since I am writing both.
    // I will update the Plugin to send `receivedAt` or `key`? 
    // Wait, `messageId` is unique.
    // I will just change the KV Key to `msg:${messageId}` for MVP. 
    // Sort order? `list` is lexicographical. UUIDs are random.
    // This means messages come in random order. That is bad for a "Notebook".
    // We want chronological.

    // Back to `msg:${timestamp}:${messageId}`.
    // I will Change Plugin to send `ack` with `{ messageIds: [...] }`.
    // In the Worker, I HAVE to scan.
    // `await env.LINE_MEMO_KV.list({ prefix: 'msg:' })`. 
    // If I limit to 1000, and just iterate to find the IDs to delete.
    // Cloudflare Workers is fast enough for this for a personal app (100s of msgs max).
    // It's acceptable for MVP.

    // Wait, I can't iterate values in `list` without `get`. 
    // `list` only gives keys and metadata.
    // I can store `messageId` in `metadata` of KV!
    // `await env.LINE_MEMO_KV.put(key, value, { metadata: { messageId: ... } })`
    // Then `list` gives me the messageId in metadata!
    // Perfect.

    // Implementation:
    // 1. Webhook: `put` with `metadata: { messageId }`.
    // 2. Pull: `list`. Get values.
    // 3. Ack: `list`. Check metadata `messageId`. If match, `delete(key)`.

    // OK.

    const list = await env.LINE_MEMO_KV.list({ prefix: 'msg:' });
    for (const key of list.keys) {
        const meta = key.metadata as { messageId: string };
        if (meta && messageIds.includes(meta.messageId)) {
            await env.LINE_MEMO_KV.delete(key.name);
            deletedCount++;
        }
    }

    return new Response(JSON.stringify({ deleted: deletedCount }), {
        headers: { 'Content-Type': 'application/json' }
    });
}

function checkAuth(request: Request, env: Env): boolean {
    const auth = request.headers.get('Authorization');
    return auth === `Bearer ${env.API_TOKEN}`;
}

async function verifySignature(channelSecret: string, signature: string, body: string): Promise<boolean> {
    const encoder = new TextEncoder();
    const key = await crypto.subtle.importKey(
        'raw',
        encoder.encode(channelSecret),
        { name: 'HMAC', hash: 'SHA-256' },
        false,
        ['sign']
    );
    const signed = await crypto.subtle.sign(
        'HMAC',
        key,
        encoder.encode(body)
    );
    const hash = btoa(String.fromCharCode(...new Uint8Array(signed)));
    return hash === signature;
}
