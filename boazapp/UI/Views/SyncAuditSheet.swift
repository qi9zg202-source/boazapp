import SwiftUI

struct SyncAuditSheet: View {
    @ObservedObject var model: HealthDashboardModel
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    statusCard
                    receipts
                    auditTrail
                }
                .padding(16)
            }
            .background(BoazPalette.black.ignoresSafeArea())
            .navigationTitle("Sync audit")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") { dismiss() }
                }
            }
        }
        .preferredColorScheme(.dark)
        .presentationDragIndicator(.visible)
    }

    private var statusCard: some View {
        BoazCard {
            VStack(alignment: .leading, spacing: 12) {
                SectionEyebrow(text: "Current state")
                CloudStatusBadge(state: model.cloudState)
                Text(explanation)
                    .font(.subheadline)
                    .foregroundStyle(BoazPalette.secondary)
                    .fixedSize(horizontal: false, vertical: true)
                HStack {
                    count("LOCAL", value: model.snapshot.localSampleCount)
                    Spacer()
                    count("PENDING", value: model.snapshot.pendingSampleCount)
                }
            }
        }
    }

    private var explanation: String {
        switch model.cloudState {
        case .localOnly: model.snapshot.localSampleCount == 0
            ? "No readable health records are stored on this iPhone yet. Cloud upload is off."
            : "\(model.snapshot.localSampleCount) readable health records are stored on this iPhone. Cloud upload is off."
        case .localSaved: "Readable records are saved on this iPhone. Cloud upload has not completed."
        case .pairingRequired: "Cloud upload needs private pairing and separate consent."
        case .queued: "Records are safely queued on this iPhone for a later upload."
        case .uploading: "A batch is being sent to the Tokyo receiver."
        case .cloudSaved: "The Tokyo receiver committed the batch to its SQLite record. Metrics may still be behind."
        case .metricsPending: "The Tokyo SQLite receipt exists. The VictoriaMetrics projection has not been confirmed."
        case .metricsCurrent: "The Tokyo SQLite receipt and server metric projection have both been confirmed."
        case .erasurePending: "Upload is off. The erasure request may still be queued on this iPhone. Wait for Tokyo to confirm active removal and managed backup expiry."
        case .activeErasureConfirmed: "Tokyo confirmed active SQLite and metrics removal and managed backup expiry."
        case .offline: "The Tokyo receiver could not be reached. Pending records remain on this iPhone."
        case .failure(let message): message
        }
    }

    private func count(_ label: String, value: Int) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(label).font(.caption2.weight(.semibold)).tracking(1.2).foregroundStyle(BoazPalette.muted)
            Text(value.formatted()).font(.title3.weight(.semibold)).monospacedDigit()
        }
    }

    private var receipts: some View {
        BoazCard {
            VStack(alignment: .leading, spacing: 14) {
                SectionEyebrow(text: "Receipts")
                timestamp("Last local import", model.snapshot.lastImportedAt)
                timestamp("Last cloud SQLite receipt", model.snapshot.lastCloudReceiptAt)
                timestamp("Last confirmed metrics projection", model.snapshot.lastProjectedAt)
                if let count = model.snapshot.lastBatchSampleCount {
                    HStack {
                        Text("Last committed batch")
                            .foregroundStyle(BoazPalette.secondary)
                        Spacer()
                        Text("\(count) records").monospacedDigit()
                    }
                    .font(.subheadline)
                }
            }
        }
    }

    private func timestamp(_ name: String, _ date: Date?) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(name).font(.caption).foregroundStyle(BoazPalette.secondary)
            Text(date?.formatted(date: .abbreviated, time: .standard) ?? "No confirmed receipt")
                .font(.subheadline.weight(.medium))
                .monospacedDigit()
                .foregroundStyle(date == nil ? BoazPalette.muted : .white)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityElement(children: .combine)
    }

    private var auditTrail: some View {
        BoazCard {
            VStack(alignment: .leading, spacing: 16) {
                SectionEyebrow(text: "Event history")
                if model.snapshot.audit.isEmpty {
                    Text("No recorded sync events yet")
                        .font(.subheadline)
                        .foregroundStyle(BoazPalette.secondary)
                } else {
                    ForEach(model.snapshot.audit.sorted(by: { $0.occurredAt > $1.occurredAt })) { entry in
                        HStack(alignment: .top, spacing: 12) {
                            Circle().fill(color(entry.severity)).frame(width: 7, height: 7).padding(.top, 6)
                            VStack(alignment: .leading, spacing: 4) {
                                Text(entry.title).font(.subheadline.weight(.semibold))
                                Text(entry.detail).font(.caption).foregroundStyle(BoazPalette.secondary)
                                Text(entry.occurredAt, format: .dateTime.month(.abbreviated).day().hour().minute().second())
                                    .font(.caption2).monospacedDigit().foregroundStyle(BoazPalette.muted)
                            }
                            Spacer(minLength: 0)
                        }
                        .accessibilityElement(children: .combine)
                    }
                }
            }
        }
    }

    private func color(_ severity: AuditSeverity) -> Color {
        switch severity {
        case .information: BoazPalette.secondary
        case .success: BoazPalette.success
        case .warning: BoazPalette.amber
        case .failure: BoazPalette.danger
        }
    }
}
