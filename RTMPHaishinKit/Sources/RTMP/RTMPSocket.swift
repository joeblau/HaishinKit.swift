import Foundation
import HaishinKit
import Network

final actor RTMPSocket {
    static let defaultWindowSizeC = Int(UInt8.max)

    enum ConnectionDisposition: Equatable {
        case keepOpen
        case close
    }

    enum Error: Swift.Error {
        case invalidState
        case endOfStream
        case connectionTimedOut
        case connectionNotEstablished(_ error: NWError?)
    }

    private var timeout: UInt64 = 15
    private var connected = false
    private var windowSizeC = RTMPSocket.defaultWindowSizeC
    private var securityLevel: StreamSocketSecurityLevel = .none
    private var totalBytesIn = 0
    private var queueBytesOut = 0
    private var totalBytesOut = 0
    private var parameters: NWParameters = .tcp
    private var connection: NWConnection? {
        didSet {
            oldValue?.viabilityUpdateHandler = nil
            oldValue?.stateUpdateHandler = nil
            oldValue?.forceCancel()
        }
    }
    private var outputs: AsyncStream<Data>.Continuation? {
        didSet {
            oldValue?.finish()
        }
    }
    private var qualityOfService: DispatchQoS = .userInitiated
    private var continuation: CheckedContinuation<Void, any Swift.Error>?
    private lazy var networkQueue = DispatchQueue(label: "com.haishinkit.HaishinKit.RTMPSocket.network", qos: qualityOfService)

    init() {
    }

    init(qualityOfService: DispatchQoS, securityLevel: StreamSocketSecurityLevel) {
        self.qualityOfService = qualityOfService
        switch securityLevel {
        case .ssLv2, .ssLv3, .tlSv1, .negotiatedSSL:
            parameters = .tls
        default:
            parameters = .tcp
        }
    }

    func connect(_ name: String, port: Int) async throws {
        guard !connected else {
            throw Error.invalidState
        }
        totalBytesIn = 0
        totalBytesOut = 0
        queueBytesOut = 0
        do {
            let connection = NWConnection(to: NWEndpoint.hostPort(host: .init(name), port: .init(integerLiteral: NWEndpoint.Port.IntegerLiteralType(port))), using: parameters)
            self.connection = connection
            try await withCheckedThrowingContinuation { (checkedContinuation: CheckedContinuation<Void, Swift.Error>) in
                self.continuation = checkedContinuation
                Task {
                    try? await Task.sleep(nanoseconds: timeout * 1_000_000_000)
                    guard let continuation else {
                        return
                    }
                    continuation.resume(throwing: Error.connectionTimedOut)
                    self.continuation = nil
                    close()
                }
                connection.stateUpdateHandler = { state in
                    Task { await self.stateDidChange(to: state) }
                }
                connection.viabilityUpdateHandler = { viability in
                    Task { await self.viabilityDidChange(to: viability) }
                }
                connection.start(queue: networkQueue)
            }
        } catch {
            throw error
        }
    }

    func send(_ data: Data) {
        guard connected else {
            return
        }
        queueBytesOut += data.count
        outputs?.yield(data)
    }

    func send(_ iterator: AnyIterator<Data>) {
        guard connected else {
            return
        }
        for data in iterator {
            queueBytesOut += data.count
            outputs?.yield(data)
        }
    }

    func send(_ chunks: [Data]) {
        guard connected else {
            return
        }
        for data in chunks {
            queueBytesOut += data.count
            outputs?.yield(data)
        }
    }

    func recv() -> AsyncStream<Data> {
        AsyncStream<Data> { continuation in
            Task {
                do {
                    while connected {
                        let data = try await recv()
                        continuation.yield(data)
                        totalBytesIn += data.count
                    }
                } catch {
                    continuation.finish()
                }
            }
        }
    }

    func close(_ error: NWError? = nil) {
        guard connection != nil else {
            return
        }
        if let continuation {
            continuation.resume(throwing: Error.connectionNotEstablished(error))
            self.continuation = nil
        }
        connected = false
        outputs = nil
        connection = nil
        continuation = nil
    }

    private func stateDidChange(to state: NWConnection.State) {
        if Self.disposition(for: state) == .close {
            switch state {
            case .failed(let error):
                logger.warn("Connection failed:", error)
                close(error)
            case .cancelled:
                logger.info("Connection cancelled.")
                close()
            default:
                break
            }
            return
        }
        switch state {
        case .ready:
            logger.info("Connection is ready.")
            // A connection can return to `.ready` after a transient `.waiting`.
            // Keep the existing output consumer in that case so queued RTMP
            // chunks remain serialized on one task and preserve wire order.
            guard !connected else {
                return
            }
            connected = true
            let (stream, continuation) = AsyncStream<Data>.makeStream()
            Task {
                do {
                    for await data in stream where connected {
                        try await send(data)
                        totalBytesOut += data.count
                        queueBytesOut -= data.count
                    }
                } catch let error as NWError {
                    logger.warn("Connection send failed:", error)
                    close(error)
                } catch {
                    logger.warn("Connection send failed:", String(describing: error))
                    close()
                }
            }
            self.outputs = continuation
            self.continuation?.resume()
            self.continuation = nil
        case .waiting(let error):
            logger.warn("Connection waiting:", error)
            // `.waiting` is explicitly nonterminal. Network.framework keeps the
            // connection alive while it waits for a usable path and can move it
            // back to `.ready` without another TCP/TLS handshake. Closing here
            // turned short app-switch and interface-transition path changes into
            // guaranteed RTMP disconnects.
        case .setup:
            logger.debug("Connection is setting up.")
        case .preparing:
            logger.debug("Connection is preparing.")
        case .failed, .cancelled:
            break
        @unknown default:
            logger.error("Unknown connection state.")
        }
    }

    private func viabilityDidChange(to viability: Bool) {
        logger.info("Connection viability changed to ", viability)
        // Viability is advisory and can recover. The NWConnection state handler
        // remains authoritative: `.failed` and `.cancelled` close the socket,
        // while temporary non-viability leaves the live flow intact.
    }

    nonisolated static func disposition(for state: NWConnection.State) -> ConnectionDisposition {
        switch state {
        case .failed, .cancelled:
            return .close
        case .setup, .waiting, .preparing, .ready:
            return .keepOpen
        @unknown default:
            return .keepOpen
        }
    }

    private func send(_ data: Data) async throws {
        return try await withCheckedThrowingContinuation { continuation in
            guard let connection else {
                continuation.resume(throwing: Error.invalidState)
                return
            }
            connection.send(content: data, completion: .contentProcessed { error in
                if let error {
                    continuation.resume(throwing: error)
                    return
                }
                continuation.resume()
            })
        }
    }

    private func recv() async throws -> Data {
        return try await withCheckedThrowingContinuation { continuation in
            guard let connection else {
                continuation.resume(throwing: Error.invalidState)
                return
            }
            connection.receive(minimumIncompleteLength: 0, maximumLength: windowSizeC) { content, _, _, error in
                if let error {
                    continuation.resume(throwing: error)
                    return
                }
                if let content {
                    continuation.resume(returning: content)
                } else {
                    continuation.resume(throwing: Error.endOfStream)
                }
            }
        }
    }
}

extension RTMPSocket: NetworkTransportReporter {
    // MARK: NetworkTransportReporter
    func makeNetworkMonitor() async -> NetworkMonitor {
        return .init(self)
    }

    func makeNetworkTransportReport() -> NetworkTransportReport {
        return .init(queueBytesOut: queueBytesOut, totalBytesIn: totalBytesIn, totalBytesOut: totalBytesOut)
    }
}
