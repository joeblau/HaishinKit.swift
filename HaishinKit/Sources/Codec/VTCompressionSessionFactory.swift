import Foundation
import VideoToolbox

/// Low-latency rate control is an optional encoder optimization. Some encoders
/// reject that specification even though ordinary real-time encoding works.
enum VTCompressionSessionFactory {
    static func make(
        encoderSpecification: CFDictionary?,
        create: (CFDictionary?, inout VTCompressionSession?) -> OSStatus
    ) throws -> VTCompressionSession {
        var session: VTCompressionSession?
        var status = create(encoderSpecification, &session)
        if status == kVTParameterErr,
           var specification = encoderSpecification as? [String: Any],
           specification[kVTVideoEncoderSpecification_EnableLowLatencyRateControl as String] as? Bool == true {
            // A failed create must not leave a retained session behind. Preserve
            // all other requirements, including a caller's hardware selection.
            if let session { VTCompressionSessionInvalidate(session) }
            session = nil
            specification.removeValue(forKey: kVTVideoEncoderSpecification_EnableLowLatencyRateControl as String)
            logger.warn("Low-latency encoder creation rejected: ", status, "; trying ordinary real-time encoding")
            status = create(specification as CFDictionary, &session)
            logger.warn("Ordinary encoder creation status: ", status)
        }
        guard status == noErr, let session else {
            if let session { VTCompressionSessionInvalidate(session) }
            throw VTSessionError.failedToCreate(status: status)
        }
        return session
    }
}
