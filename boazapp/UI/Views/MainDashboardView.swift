import SwiftUI
import UIKit

struct MainDashboardView: View {
    @ObservedObject var model: HealthDashboardModel
    @State private var showsAudit = false
    @State private var showsSettings = false
    @Environment(\.dynamicTypeSize) private var typeSize

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 16) {
                header
                hero
                SleepSectionView(sleep: model.snapshot.sleep)
                FitnessSectionView(fitness: model.snapshot.fitness)
                VitalsSectionView(vitals: model.snapshot.vitals)
                Text("Boaz Health · Data shown from this device's readable Apple Health records")
                    .font(.caption2)
                    .foregroundStyle(BoazPalette.muted)
                    .frame(maxWidth: .infinity)
                    .padding(.top, 8)
                    .padding(.bottom, 28)
            }
            .padding(.horizontal, 16)
            .padding(.top, 16)
        }
        .background(BoazPalette.black.ignoresSafeArea())
        .preferredColorScheme(.dark)
        .sheet(isPresented: $showsAudit) { SyncAuditSheet(model: model) }
        .sheet(isPresented: $showsSettings) { HealthSettingsSheet(model: model) }
        .alert("Health sync needs attention", isPresented: Binding(
            get: { model.errorMessage != nil },
            set: { if !$0 { model.errorMessage = nil } }
        )) {
            Button("OK", role: .cancel) { model.errorMessage = nil }
        } message: {
            Text(model.errorMessage ?? "Unknown error")
        }
        .task { await model.onRefresh?() }
        .onChange(of: model.syncPhase) { _, phase in
            if phase == .finished { UINotificationFeedbackGenerator().notificationOccurred(.success) }
            if phase == .failed { UINotificationFeedbackGenerator().notificationOccurred(.error) }
        }
    }

    @ViewBuilder private var header: some View {
        if typeSize.isAccessibilitySize {
            VStack(alignment: .leading, spacing: 12) {
                brand
                HStack(spacing: 10) { auditButton; Spacer(); settingsButton }
            }
            .padding(.bottom, 6)
        } else {
            HStack(alignment: .center, spacing: 10) {
                brand
                Spacer(minLength: 8)
                auditButton
                settingsButton
            }
            .padding(.bottom, 6)
        }
    }

    private var brand: some View {
        VStack(alignment: .leading, spacing: 3) {
            Text("BOAZ")
                .font(.system(.title2, design: .rounded, weight: .bold))
                .tracking(2.4)
                .foregroundStyle(.white)
            Text("HEALTH")
                .font(.caption2.weight(.medium))
                .tracking(3.5)
                .foregroundStyle(BoazPalette.secondary)
        }
    }

    private var auditButton: some View {
        Button { showsAudit = true } label: { CloudStatusBadge(state: model.cloudState) }
            .buttonStyle(.plain)
            .accessibilityHint("Shows sync receipts and audit history")
    }

    private var settingsButton: some View {
        Button { showsSettings = true } label: {
            Image(systemName: "gearshape")
                .font(.system(size: 17, weight: .medium))
                .foregroundStyle(BoazPalette.secondary)
                .frame(width: 44, height: 44)
                .background(BoazPalette.card, in: Circle())
        }
        .accessibilityLabel("Health and cloud settings")
    }

    private var hero: some View {
        BoazCard {
            VStack(spacing: 8) {
                DampenedSyncButton(phase: model.syncPhase, cloudState: model.cloudState) {
                    Task { await model.onSync?() }
                }
                .disabled(model.onSync == nil)
                if let importedAt = model.snapshot.lastImportedAt {
                    Text("LOCAL IMPORT  \(importedAt.formatted(date: .abbreviated, time: .standard))")
                        .font(.system(size: 10, weight: .medium, design: .monospaced))
                        .tracking(0.7)
                        .foregroundStyle(BoazPalette.secondary)
                } else {
                    Text("NO LOCAL IMPORT YET")
                        .font(.system(size: 10, weight: .medium))
                        .tracking(1.2)
                        .foregroundStyle(BoazPalette.muted)
                }
                Text("\(model.snapshot.localSampleCount.formatted()) local records · \(model.snapshot.pendingSampleCount.formatted()) pending upload")
                    .font(.caption2)
                    .foregroundStyle(BoazPalette.muted)
                    .monospacedDigit()
                if !model.uploadConsentGranted {
                    Text("Cloud upload is off")
                        .font(.caption.weight(.medium))
                        .foregroundStyle(BoazPalette.secondary)
                        .padding(.top, 2)
                }
            }
            .frame(maxWidth: .infinity)
        }
    }
}

struct CloudStatusBadge: View {
    let state: CloudDisplayState

    private var appearance: (String, Color) {
        switch state {
        case .localOnly: ("LOCAL ONLY", BoazPalette.secondary)
        case .localSaved: ("LOCAL SAVED", BoazPalette.success)
        case .pairingRequired: ("PAIRING NEEDED", BoazPalette.amber)
        case .queued: ("UPLOAD QUEUED", BoazPalette.amber)
        case .uploading: ("UPLOADING", BoazPalette.amber)
        case .cloudSaved: ("CLOUD SAVED", BoazPalette.success)
        case .metricsPending: ("METRICS PENDING", BoazPalette.amber)
        case .metricsCurrent: ("METRICS CURRENT", BoazPalette.success)
        case .erasurePending: ("ERASURE PENDING", BoazPalette.amber)
        case .activeErasureConfirmed: ("ACTIVE DATA REMOVED", BoazPalette.success)
        case .offline: ("OFFLINE", BoazPalette.amber)
        case .failure: ("SYNC ISSUE", BoazPalette.danger)
        }
    }

    var body: some View {
        HStack(spacing: 6) {
            Circle().fill(appearance.1).frame(width: 6, height: 6)
            Text(appearance.0)
                .font(.system(.caption2, design: .rounded, weight: .bold))
                .tracking(0.8)
                .lineLimit(1)
                .minimumScaleFactor(0.75)
        }
        .foregroundStyle(appearance.1)
        .padding(.horizontal, 9)
        .frame(height: 30)
        .background(BoazPalette.card, in: Capsule())
        .overlay(Capsule().strokeBorder(BoazPalette.border, lineWidth: 0.5))
        .accessibilityElement(children: .combine)
        .accessibilityLabel("Sync status: \(appearance.0)")
    }
}

#Preview("Fixture data — not live") {
    MainDashboardView(model: HealthDashboardModel(snapshot: DashboardSnapshot(
        sleep: SleepDisplay(startedAt: .now.addingTimeInterval(-8 * 3600), endedAt: .now,
                            totalMinutes: 427, deepMinutes: 78, coreMinutes: 238,
                            remMinutes: 111, awakeMinutes: 19, inBedMinutes: 446,
                            sourceCount: 8, wristTemperatureC: 35.8, overnightHeartRate: 55),
        fitness: FitnessDisplay(day: .now, moveKcal: 512, moveGoalKcal: 600,
                                exerciseMinutes: 42, exerciseGoalMinutes: 30,
                                standHours: 10, standGoalHours: 12, steps: 8342,
                                recentWorkouts: [WorkoutDisplay(id: "fixture-workout", title: "Swimming",
                                                                startedAt: .now.addingTimeInterval(-86_400), durationMinutes: 48,
                                                                energyKcal: 360)]),
        vitals: VitalsDisplay(heartRate: 64, restingHeartRate: 56, oxygenPercent: 98,
                              weightKg: 72.3, hrvMilliseconds: 48),
        localSampleCount: 1428,
        pendingSampleCount: 14,
        lastImportedAt: .now,
        audit: [SyncAuditEntry(id: "fixture", occurredAt: .now, title: "Local import",
                               detail: "Fixture only", severity: .success)]
    )))
}
