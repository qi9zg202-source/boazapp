import SwiftUI

struct HealthSettingsSheet: View {
    @ObservedObject var model: HealthDashboardModel
    @Environment(\.dismiss) private var dismiss
    @State private var serverURL = ""
    @State private var oneTimeCode = ""
    @State private var acceptsUpload = false
    @State private var actionRunning = false
    @State private var confirmsErasure = false

    private var pairingURL: URL? {
        guard let url = URL(string: serverURL.trimmingCharacters(in: .whitespacesAndNewlines)),
              url.scheme?.lowercased() == "https",
              let host = url.host?.lowercased(),
              host.hasSuffix(".ts.net"),
              url.user == nil,
              url.password == nil,
              url.query == nil,
              url.fragment == nil
        else { return nil }
        return url
    }

    var body: some View {
        NavigationStack {
            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    healthCard
                    uploadCard
                    if model.isPaired { consentCard }
                    if model.isPaired || model.cloudState == .erasurePending { controlsCard }
                }
                .padding(16)
            }
            .background(BoazPalette.black.ignoresSafeArea())
            .navigationTitle("Health settings")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) { Button("Done") { dismiss() } }
            }
        }
        .preferredColorScheme(.dark)
        .presentationDragIndicator(.visible)
        .confirmationDialog("Request cloud health erasure?", isPresented: $confirmsErasure, titleVisibility: .visible) {
            Button("Request cloud erasure", role: .destructive) {
                Task {
                    actionRunning = true
                    await model.onEraseCloud?()
                    actionRunning = false
                }
            }
        } message: {
            Text("The Tokyo service removes active SQLite and metrics data asynchronously and revokes this device's credential. Encrypted backups expire within 30 days. Local Health records remain; pairing and enabling upload again can resend them. Wait for the service to confirm active removal.")
        }
        .onDisappear { oneTimeCode = "" }
    }

    private var healthCard: some View {
        BoazCard {
            VStack(alignment: .leading, spacing: 12) {
                SectionEyebrow(text: "Apple Health")
                Text("Read health records on this iPhone")
                    .font(.headline)
                Text("Health access only populates the local ledger. It does not enable cloud upload. Apple Health can return an empty result when there are no records or when read access is limited.")
                    .font(.subheadline)
                    .foregroundStyle(BoazPalette.secondary)
                    .fixedSize(horizontal: false, vertical: true)
                Button {
                    Task { await model.onRequestHealthAccess?() }
                } label: {
                    actionLabel("Review Health access", symbol: "heart.text.square")
                }
                .buttonStyle(.plain)
                .disabled(model.onRequestHealthAccess == nil || actionRunning)
            }
        }
    }

    private var uploadCard: some View {
        BoazCard {
            VStack(alignment: .leading, spacing: 14) {
                SectionEyebrow(text: "Private Tokyo upload")
                HStack(spacing: 8) {
                    Circle()
                        .fill(model.uploadConsentGranted ? BoazPalette.success : BoazPalette.secondary)
                        .frame(width: 7, height: 7)
                    Text(model.uploadConsentGranted ? "UPLOAD ENABLED" : "UPLOAD OFF")
                        .font(.caption.weight(.bold))
                        .tracking(1)
                }
                Text("Pairing establishes a private Tailscale connection. It does not enable upload. A separate consent step appears after pairing.")
                    .font(.subheadline)
                    .foregroundStyle(BoazPalette.secondary)
                    .fixedSize(horizontal: false, vertical: true)

                if model.cloudState == .erasurePending {
                    Text("Cloud erasure is in progress or waiting for the private link. Pairing is available again after Tokyo confirms completion.")
                        .font(.subheadline)
                        .foregroundStyle(BoazPalette.amber)
                        .fixedSize(horizontal: false, vertical: true)
                } else if !model.isPaired {
                    VStack(alignment: .leading, spacing: 10) {
                        Text("TAILSCALE HTTPS ADDRESS")
                            .font(.caption2.weight(.semibold))
                            .tracking(1)
                            .foregroundStyle(BoazPalette.muted)
                        TextField("https://device.tailnet.ts.net", text: $serverURL)
                            .textInputAutocapitalization(.never)
                            .autocorrectionDisabled()
                            .keyboardType(.URL)
                            .textContentType(.URL)
                            .accessibilityLabel("Private Tokyo server address")
                            .modifier(InputSurface())
                        Text("ONE-TIME PAIRING CODE")
                            .font(.caption2.weight(.semibold))
                            .tracking(1)
                            .foregroundStyle(BoazPalette.muted)
                        SecureField("Pairing code", text: $oneTimeCode)
                            .textInputAutocapitalization(.never)
                            .autocorrectionDisabled()
                            .textContentType(.oneTimeCode)
                            .accessibilityLabel("One-time pairing code")
                            .modifier(InputSurface())
                        Button {
                            guard let endpoint = pairingURL else { return }
                            Task {
                                actionRunning = true
                                await model.onPair?(endpoint, oneTimeCode.trimmingCharacters(in: .whitespacesAndNewlines))
                                oneTimeCode = ""
                                actionRunning = false
                            }
                        } label: {
                            actionLabel("Pair this iPhone", symbol: "lock.shield")
                        }
                        .buttonStyle(.plain)
                        .disabled(pairingURL == nil || oneTimeCode.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || model.onPair == nil || actionRunning)
                    }
                    .padding(.top, 4)
                } else {
                    Text("This iPhone is paired. Upload remains under your separate consent control below.")
                        .font(.caption)
                        .foregroundStyle(BoazPalette.secondary)
                }
            }
        }
    }

    private var consentCard: some View {
        BoazCard {
            VStack(alignment: .leading, spacing: 12) {
                SectionEyebrow(text: "Cloud upload consent")
                Text("Data sent to Tokyo")
                    .font(.headline)
                Text("Sleep stages, wrist temperature, night heart and breathing rates, blood oxygen; heart rate, HRV, blood pressure, weight, body fat and BMI; activity rings, steps, flights and workouts, including associated heart rate and events. Records include sample times, source app and available device details. Only readable records are sent.")
                    .font(.subheadline)
                    .foregroundStyle(BoazPalette.secondary)
                    .fixedSize(horizontal: false, vertical: true)
                Text("Destination: your private Tokyo SQLite receiver through Tailscale HTTPS. Its separate VictoriaMetrics projection is derived from those records. Records remain while consent is active. On deletion request, active removal is asynchronous and encrypted backup copies expire within 30 days.")
                    .font(.subheadline)
                    .foregroundStyle(BoazPalette.secondary)
                    .fixedSize(horizontal: false, vertical: true)
                if model.uploadConsentGranted {
                    Label("Upload consent is enabled", systemImage: "checkmark.circle.fill")
                        .font(.subheadline.weight(.medium))
                        .foregroundStyle(BoazPalette.success)
                } else {
                    Toggle(isOn: $acceptsUpload) {
                        Text("I agree to upload these health records to my private Tokyo service")
                            .font(.subheadline)
                    }
                    .tint(BoazPalette.success)
                    Button {
                        Task {
                            actionRunning = true
                            await model.onSetUploadConsent?(true)
                            actionRunning = false
                        }
                    } label: {
                        actionLabel("Enable cloud upload", symbol: "arrow.up.circle")
                    }
                    .buttonStyle(.plain)
                    .disabled(!acceptsUpload || model.onSetUploadConsent == nil || actionRunning)
                }
            }
        }
    }

    private var controlsCard: some View {
        BoazCard {
            VStack(alignment: .leading, spacing: 12) {
                SectionEyebrow(text: "Control and deletion")
                if model.cloudState == .erasurePending {
                    Button {
                        Task {
                            actionRunning = true
                            await model.onEraseCloud?()
                            actionRunning = false
                        }
                    } label: {
                        actionLabel("Retry erasure request", symbol: "arrow.clockwise")
                    }
                    .buttonStyle(.plain)
                    .disabled(model.onEraseCloud == nil || actionRunning)
                    Button {
                        Task { await model.onRefresh?() }
                    } label: {
                        actionLabel("Check erasure status", symbol: "checkmark.shield")
                    }
                    .buttonStyle(.plain)
                }
                if model.uploadConsentGranted {
                    Button {
                        Task {
                            actionRunning = true
                            if let onSetUploadConsent = model.onSetUploadConsent {
                                await onSetUploadConsent(false)
                            } else {
                                await model.onStopUpload?()
                            }
                            actionRunning = false
                        }
                    } label: {
                        actionLabel("Stop future upload", symbol: "pause.circle")
                    }
                    .buttonStyle(.plain)
                    .disabled((model.onSetUploadConsent == nil && model.onStopUpload == nil) || actionRunning)
                    Text("Stopping upload keeps the local ledger and existing Tokyo records. A batch already in flight may finish. You can request cloud erasure below.")
                        .font(.caption)
                        .foregroundStyle(BoazPalette.secondary)
                }
                if model.cloudState != .erasurePending {
                    Button(role: .destructive) { confirmsErasure = true } label: {
                        actionLabel("Request cloud erasure", symbol: "trash")
                            .foregroundStyle(BoazPalette.danger)
                    }
                    .buttonStyle(.plain)
                    .disabled(model.onEraseCloud == nil || actionRunning)
                }
                Text("Active cloud removal is asynchronous. Encrypted backup copies expire within 30 days. Local Health records remain; pairing and enabling upload again can resend them. The audit view shows confirmed progress.")
                    .font(.caption)
                    .foregroundStyle(BoazPalette.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
    }

    private func actionLabel(_ text: String, symbol: String) -> some View {
        HStack(spacing: 10) {
            Image(systemName: symbol)
            Text(text).font(.subheadline.weight(.semibold))
            Spacer()
            Image(systemName: "chevron.right").font(.caption.weight(.semibold))
        }
        .foregroundStyle(.white)
        .padding(14)
        .background(BoazPalette.inset, in: RoundedRectangle(cornerRadius: 12))
    }
}

private struct InputSurface: ViewModifier {
    func body(content: Content) -> some View {
        content
            .font(.subheadline)
            .padding(12)
            .background(BoazPalette.inset, in: RoundedRectangle(cornerRadius: 10))
            .overlay(RoundedRectangle(cornerRadius: 10)
                .strokeBorder(BoazPalette.border, lineWidth: 0.5))
    }
}
