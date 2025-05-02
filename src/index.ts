import { EventSource } from 'eventsource';
import fetch from 'node-fetch';

interface JsonRpcRequest {
    jsonrpc: '2.0';
    id?: number;
    method: string;
    params?: any;
}

interface Tool {
    name: string;
    description: string;
    inputSchema?: {
        type: string;
        properties: Record<string, any>;
        required?: string[];
    };
}

interface ToolsListResponse {
    tools: Tool[];
    nextCursor?: string;
}

class MCPClient {
    private eventSource: EventSource | null = null;
    private readonly sseUrl: string;
    private messageEndpoint: string | null = null;
    private requestId: number = 1;
    private sessionId: string | null = null;
    private initialized: boolean = false;

    constructor() {
        this.sseUrl = 'https://agent.mcpify.ai/sse?server=67fa65f5-eb5d-4f5c-b902-68108f4fb2c8';
    }

    private async sendJsonRpcRequest(request: JsonRpcRequest): Promise<any> {
        if (!this.messageEndpoint) {
            throw new Error('No message endpoint available - SSE connection not established');
        }

        if (!this.initialized && request.method !== 'initialize' && request.method !== 'notifications/initialized') {
            throw new Error('Client not initialized');
        }

        const response = await fetch(this.messageEndpoint, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(request)
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const contentType = response.headers.get('content-type');
        if (contentType && contentType.includes('application/json')) {
            return await response.json();
        } else {
            // For non-JSON responses (like "Accepted"), return the text
            const text = await response.text();
            return { result: text };
        }
    }

    private async initialize(): Promise<void> {
        console.log('Initializing MCP client...');
        
        const initRequest: JsonRpcRequest = {
            jsonrpc: '2.0',
            id: this.requestId++,
            method: 'initialize',
            params: {
                protocolVersion: '2024-11-05',
                capabilities: {
                    roots: {
                        listChanged: true
                    },
                    sampling: {}
                },
                clientInfo: {
                    name: 'MCPify Client',
                    version: '1.0.0'
                }
            }
        };

        const initResponse = await this.sendJsonRpcRequest(initRequest);
        console.log('Initialization response:', initResponse);

        // Send initialized notification
        const initializedNotification: JsonRpcRequest = {
            jsonrpc: '2.0',
            method: 'notifications/initialized'
        };

        await this.sendJsonRpcRequest(initializedNotification);
        this.initialized = true;
        console.log('Client initialized successfully');

        // Wait a moment for the server to process initialization
        await new Promise(resolve => setTimeout(resolve, 1000));
    }

    public async connect(): Promise<void> {
        console.log('Connecting to MCP server via SSE...');
        
        return new Promise((resolve, reject) => {
            try {
                this.eventSource = new EventSource(this.sseUrl);
            } catch (error) {
                reject(new Error(`Failed to create EventSource: ${error}`));
                return;
            }

            const es = this.eventSource;

            es.onopen = () => {
                console.log('SSE connection established');
            };

            es.onerror = (error) => {
                console.error('SSE connection error:', error);
                reject(new Error('SSE connection failed'));
            };

            // Listen for the endpoint event specifically
            es.addEventListener('endpoint', async (event: MessageEvent) => {
                try {
                    // Handle both JSON and plain text formats
                    let endpoint: string;
                    try {
                        const data = JSON.parse(event.data);
                        endpoint = data.url;
                        this.sessionId = data.sessionId;
                    } catch {
                        // If not JSON, assume it's a direct URL
                        endpoint = event.data.trim();
                    }

                    // Ensure the endpoint is a full URL
                    this.messageEndpoint = endpoint.startsWith('http') 
                        ? endpoint 
                        : new URL(endpoint, this.sseUrl).toString();
                        
                    console.log('Received message endpoint:', this.messageEndpoint);
                    
                    // Initialize after getting the endpoint
                    await this.initialize();
                    resolve();
                } catch (error) {
                    console.error('Error processing endpoint event:', error);
                    reject(error);
                }
            });

            // Handle other SSE messages
            es.onmessage = (event: MessageEvent) => {
                try {
                    const data = JSON.parse(event.data);
                    console.log('Received SSE message:', data);
                } catch (error) {
                    // Don't error on non-JSON messages
                    console.log('Received raw SSE message:', event.data);
                }
            };

            // Set a timeout for the initial connection
            setTimeout(() => {
                if (!this.messageEndpoint) {
                    reject(new Error('Timeout waiting for endpoint event'));
                    this.disconnect();
                }
            }, 10000);
        });
    }

    public async listTools(cursor?: string): Promise<ToolsListResponse> {
        if (!this.initialized) {
            throw new Error('Client must be initialized before listing tools');
        }

        const requestId = this.requestId++;
        const request: JsonRpcRequest = {
            jsonrpc: '2.0',
            id: requestId,
            method: 'tools/list',
            params: cursor ? { cursor } : {}
        };

        // Send the request but don't wait for its response
        await this.sendJsonRpcRequest(request);
        
        // The actual response will come through the SSE connection
        return new Promise((resolve, reject) => {
            const timeoutId = setTimeout(() => {
                this.eventSource?.removeEventListener('message', messageHandler);
                reject(new Error('Timeout waiting for tools list response'));
            }, 5000);

            const messageHandler = (event: MessageEvent) => {
                try {
                    const data = JSON.parse(event.data);
                    // Match the response to our request ID
                    if (data.jsonrpc === '2.0' && data.id === requestId && data.result?.tools) {
                        // Remove the handler and timeout
                        this.eventSource?.removeEventListener('message', messageHandler);
                        clearTimeout(timeoutId);

                        console.log('\nAvailable tools:');
                        data.result.tools.forEach((tool: Tool) => {
                            console.log(`- ${tool.name}: ${tool.description}`);
                        });

                        resolve(data.result);
                    }
                } catch (error) {
                    console.log('Error processing message:', error);
                }
            };

            this.eventSource?.addEventListener('message', messageHandler);
        });
    }

    public async searchFlights(params: {
        sourceLocation: string;
        destinationLocation: string;
        departureDate: string;
        returnDate: string;
    }): Promise<any> {
        if (!this.initialized) {
            throw new Error('Client must be initialized before calling tools');
        }

        const requestId = this.requestId++;
        const request: JsonRpcRequest = {
            jsonrpc: '2.0',
            id: requestId,
            method: 'tools/call',
            params: {
                name: 'search_flights',
                arguments: params
            }
        };

        // Send the request but don't wait for its response
        await this.sendJsonRpcRequest(request);
        
        // The actual response will come through the SSE connection
        return new Promise((resolve, reject) => {
            const timeoutId = setTimeout(() => {
                this.eventSource?.removeEventListener('message', messageHandler);
                reject(new Error('Timeout waiting for search_flights response'));
            }, 10000);

            const messageHandler = (event: MessageEvent) => {
                try {
                    const data = JSON.parse(event.data);
                    // Match the response to our request ID
                    if (data.jsonrpc === '2.0' && data.id === requestId) {
                        // Remove the handler and timeout
                        this.eventSource?.removeEventListener('message', messageHandler);
                        clearTimeout(timeoutId);

                        if (data.error) {
                            reject(new Error(`Flight search failed: ${data.error.message}`));
                        } else {
                            console.log('\nFlight search results:', data.result);
                            resolve(data.result);
                        }
                    }
                } catch (error) {
                    console.log('Error processing message:', error);
                }
            };

            this.eventSource?.addEventListener('message', messageHandler);
        });
    }

    public disconnect(): void {
        if (this.eventSource) {
            this.eventSource.close();
            console.log('Disconnected from MCP server');
        }
        this.messageEndpoint = null;
        this.sessionId = null;
        this.initialized = false;
    }
}

async function main() {
    const client = new MCPClient();
    
    try {
        await client.connect();
        console.log('Connection and initialization complete');
        
        // List available tools
        await client.listTools();

        // Search for flights
        console.log('\nSearching for flights...');
        await client.searchFlights({
            sourceLocation: 'BLR',
            destinationLocation: 'COK',
            departureDate: '2025-05-05',
            returnDate: '2025-05-07'
        });

        // Handle graceful shutdown
        process.on('SIGINT', () => {
            client.disconnect();
            process.exit(0);
        });
    } catch (error) {
        console.error('Failed to start client:', error);
        process.exit(1);
    }
}

main();