import AppKit
import Foundation

let size = 1024
guard let bitmap = NSBitmapImageRep(
    bitmapDataPlanes: nil, pixelsWide: size, pixelsHigh: size,
    bitsPerSample: 8, samplesPerPixel: 4, hasAlpha: true,
    isPlanar: false, colorSpaceName: .deviceRGB, bytesPerRow: 0, bitsPerPixel: 0
), let context = NSGraphicsContext(bitmapImageRep: bitmap) else {
    fatalError("Could not create app icon bitmap")
}
NSGraphicsContext.saveGraphicsState()
NSGraphicsContext.current = context
let background = NSColor(calibratedRed: 0.035, green: 0.058, blue: 0.085, alpha: 1)
background.setFill()
NSRect(x: 0, y: 0, width: size, height: size).fill()

let outer = NSBezierPath(ovalIn: NSRect(x: 130, y: 130, width: 764, height: 764))
outer.lineWidth = 19
NSColor(calibratedRed: 0.16, green: 0.29, blue: 0.33, alpha: 1).setStroke()
outer.stroke()

let inner = NSBezierPath(ovalIn: NSRect(x: 166, y: 166, width: 692, height: 692))
inner.lineWidth = 7
NSColor(calibratedRed: 0.10, green: 0.72, blue: 0.68, alpha: 0.38).setStroke()
inner.stroke()

let wave = NSBezierPath()
wave.lineWidth = 30
wave.lineCapStyle = .round
wave.lineJoinStyle = .round
wave.move(to: NSPoint(x: 215, y: 510))
for point in [
    NSPoint(x: 385, y: 510), NSPoint(x: 435, y: 565),
    NSPoint(x: 483, y: 390), NSPoint(x: 552, y: 640),
    NSPoint(x: 602, y: 510), NSPoint(x: 810, y: 510)
] { wave.line(to: point) }
NSColor(calibratedRed: 0.23, green: 0.94, blue: 0.84, alpha: 1).setStroke()
wave.stroke()

NSGraphicsContext.restoreGraphicsState()
let output = URL(fileURLWithPath: CommandLine.arguments[1])
let temporaryJPEG = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString + ".jpg")
guard let data = bitmap.representation(using: .jpeg, properties: [.compressionFactor: 1.0]) else {
    fatalError("JPEG encoding failed")
}
try data.write(to: temporaryJPEG, options: .atomic)
defer { try? FileManager.default.removeItem(at: temporaryJPEG) }
let conversion = Process()
conversion.executableURL = URL(fileURLWithPath: "/usr/bin/sips")
conversion.arguments = ["-s", "format", "png", temporaryJPEG.path, "--out", output.path]
try conversion.run()
conversion.waitUntilExit()
guard conversion.terminationStatus == 0 else { fatalError("Opaque PNG conversion failed") }
