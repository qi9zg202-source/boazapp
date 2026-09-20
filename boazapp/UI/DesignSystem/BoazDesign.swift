import SwiftUI
import UIKit

enum BoazPalette {
    static let black = Color.black
    static let card = Color(red: 17 / 255, green: 19 / 255, blue: 23 / 255)
    static let inset = Color(red: 22 / 255, green: 25 / 255, blue: 31 / 255)
    static let border = Color(red: 35 / 255, green: 39 / 255, blue: 47 / 255)
    static let secondary = Color(red: 148 / 255, green: 163 / 255, blue: 184 / 255)
    static let muted = Color(red: 100 / 255, green: 116 / 255, blue: 139 / 255)
    static let success = Color(red: 16 / 255, green: 185 / 255, blue: 129 / 255)
    static let amber = Color(red: 245 / 255, green: 158 / 255, blue: 11 / 255)
    static let danger = Color(red: 239 / 255, green: 68 / 255, blue: 68 / 255)
}

struct BoazCard<Content: View>: View {
    let content: Content

    init(@ViewBuilder content: () -> Content) { self.content = content() }

    var body: some View {
        content
            .padding(20)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(BoazPalette.card, in: RoundedRectangle(cornerRadius: 22, style: .continuous))
            .overlay(RoundedRectangle(cornerRadius: 22, style: .continuous)
                .strokeBorder(BoazPalette.border, lineWidth: 0.5))
    }
}

struct SectionEyebrow: View {
    let text: String

    var body: some View {
        Text(text.uppercased())
            .font(.system(.caption2, design: .rounded, weight: .semibold))
            .tracking(2.1)
            .foregroundStyle(BoazPalette.secondary)
    }
}

struct ValueLabel: View {
    let value: String
    let unit: String
    var size: CGFloat = 36
    @ScaledMetric(relativeTo: .largeTitle) private var typeScale: CGFloat = 1

    var body: some View {
        HStack(alignment: .firstTextBaseline, spacing: 5) {
            Text(value)
                .font(.system(size: size * typeScale, weight: .semibold, design: .rounded))
                .monospacedDigit()
                .minimumScaleFactor(0.65)
                .lineLimit(1)
                .foregroundStyle(.white)
            if !unit.isEmpty {
                Text(unit)
                    .font(.caption.weight(.medium))
                    .foregroundStyle(BoazPalette.secondary)
            }
        }
        .accessibilityElement(children: .combine)
    }
}

struct DampenedSyncButton: View {
    let phase: SyncPhase
    let cloudState: CloudDisplayState
    let action: () -> Void

    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    @State private var rotating = false

    private var label: String {
        switch phase {
        case .collecting: "COLLECTING HEALTH"
        case .committing: "COMMITTING TO TOKYO"
        case .failed: "TRY AGAIN"
        case .finished:
            switch cloudState {
            case .metricsCurrent: "SYNCHRONIZED"
            case .cloudSaved, .metricsPending: "CLOUD SAVED"
            default: "LOCAL SAVED"
            }
        case .idle: "SYNC NOW"
        }
    }

    var body: some View {
        Button(action: action) {
            VStack(spacing: 14) {
                ZStack {
                    Circle()
                        .strokeBorder(BoazPalette.border, lineWidth: 1)
                        .frame(width: 112, height: 112)
                    Circle()
                        .strokeBorder(phase.isWorking ? BoazPalette.amber.opacity(0.55) : BoazPalette.success.opacity(0.25), lineWidth: 1)
                        .frame(width: 88, height: 88)
                    Image(systemName: "arrow.triangle.2.circlepath")
                        .font(.system(size: 30, weight: .light))
                        .foregroundStyle(phase.isWorking ? BoazPalette.amber : .white)
                        .rotationEffect(.degrees(rotating ? 360 : 0))
                        .animation(reduceMotion ? nil : .linear(duration: 2).repeatForever(autoreverses: false), value: rotating)
                }
                Text(label)
                    .font(.system(.caption2, design: .rounded, weight: .bold))
                    .tracking(2)
                    .foregroundStyle(.white)
                    .contentTransition(.opacity)
            }
            .frame(maxWidth: .infinity)
            .padding(.vertical, 26)
            .contentShape(Rectangle())
        }
        .buttonStyle(RigidPressStyle())
        .disabled(phase.isWorking)
        .accessibilityLabel(label)
        .accessibilityHint("Collects readable Apple Health data and saves it locally. Upload occurs only if enabled in settings.")
        .onAppear { rotating = phase.isWorking && !reduceMotion }
        .onChange(of: phase) { _, value in rotating = value.isWorking && !reduceMotion }
        .onChange(of: reduceMotion) { _, value in rotating = phase.isWorking && !value }
    }
}

private struct RigidPressStyle: ButtonStyle {
    @Environment(\.accessibilityReduceMotion) private var reduceMotion

    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .scaleEffect(configuration.isPressed && !reduceMotion ? 0.96 : 1)
            .animation(reduceMotion ? nil : .spring(response: 0.35, dampingFraction: 0.82), value: configuration.isPressed)
            .onChange(of: configuration.isPressed) { _, pressed in
                if pressed { UIImpactFeedbackGenerator(style: .rigid).impactOccurred() }
            }
    }
}
