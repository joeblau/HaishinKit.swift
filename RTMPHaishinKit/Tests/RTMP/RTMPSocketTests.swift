import Network
import Testing

@testable import RTMPHaishinKit

@Suite struct RTMPSocketTests {
    @Test func waitingStateKeepsConnectionOpen() {
        let state = NWConnection.State.waiting(.posix(.ENETDOWN))

        #expect(RTMPSocket.disposition(for: state) == .keepOpen)
    }

    @Test func terminalStatesCloseConnection() {
        let error = NWError.posix(.ECONNRESET)

        #expect(RTMPSocket.disposition(for: .failed(error)) == .close)
        #expect(RTMPSocket.disposition(for: .cancelled) == .close)
    }
}
