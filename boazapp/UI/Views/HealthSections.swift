import SwiftUI

private func number(_ value: Double?, digits: Int = 0) -> String {
    guard let value, value.isFinite else { return "—" }
    return value.formatted(.number.precision(.fractionLength(digits)))
}

private func duration(_ minutes: Double?) -> String {
    guard let minutes, minutes.isFinite, minutes >= 0 else { return "—" }
    let whole = Int(minutes.rounded())
    return "\(whole / 60)h \(whole % 60)m"
}

private struct Datum: View {
    let title: String
    let value: String
    let unit: String
    var footnote: String? = nil

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(title)
                .font(.system(size: 12, weight: .medium))
                .foregroundStyle(BoazPalette.secondary)
            ValueLabel(value: value, unit: unit, size: 24)
            if let footnote {
                Text(footnote)
                    .font(.system(size: 11))
                    .foregroundStyle(BoazPalette.muted)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(14)
        .background(BoazPalette.inset, in: RoundedRectangle(cornerRadius: 14))
        .accessibilityElement(children: .combine)
    }
}

struct SleepSectionView: View {
    let sleep: SleepDisplay?
    @Environment(\.dynamicTypeSize) private var typeSize

    var body: some View {
        BoazCard {
            VStack(alignment: .leading, spacing: 18) {
                HStack(alignment: .firstTextBaseline) {
                    SectionEyebrow(text: "Sleep / latest completed session")
                    Spacer(minLength: 8)
                    if let endedAt = sleep?.endedAt {
                        Text(endedAt, format: .dateTime.month(.abbreviated).day())
                            .font(.caption)
                            .foregroundStyle(BoazPalette.muted)
                    }
                }

                if let sleep {
                    HStack(alignment: .firstTextBaseline, spacing: 12) {
                        ValueLabel(value: duration(sleep.totalMinutes), unit: "asleep", size: 38)
                        Spacer(minLength: 0)
                        if let efficiency = sleep.efficiencyPercent {
                            VStack(alignment: .trailing, spacing: 3) {
                                Text(number(efficiency))
                                    .font(.system(size: 22, weight: .semibold, design: .rounded))
                                    .monospacedDigit()
                                Text("% EFFICIENCY")
                                    .font(.system(size: 9, weight: .semibold))
                                    .tracking(1)
                                    .foregroundStyle(BoazPalette.secondary)
                            }
                        }
                    }
                    if sleep.efficiencyPercent == nil {
                        Text(sleep.inBedMinutes == nil
                             ? "Efficiency unavailable: no in-bed record"
                             : "Efficiency unavailable: in-bed record does not cover sleep")
                            .font(.caption)
                            .foregroundStyle(BoazPalette.muted)
                    }
                    if typeSize.isAccessibilitySize {
                        VStack(spacing: 8) { stages(sleep) }
                    } else {
                        LazyVGrid(columns: [.init(.flexible(), spacing: 8), .init(.flexible(), spacing: 8)], spacing: 8) { stages(sleep) }
                    }
                    if sleep.wristTemperatureC != nil || sleep.overnightHeartRate != nil || sleep.overnightRespiratoryRate != nil || sleep.overnightOxygenPercent != nil || sleep.overnightHRVMilliseconds != nil {
                        Rectangle().fill(BoazPalette.border).frame(height: 0.5)
                        Text("NIGHT OBSERVATIONS")
                            .font(.system(size: 10, weight: .semibold))
                            .tracking(1.4)
                            .foregroundStyle(BoazPalette.muted)
                        if let temperature = sleep.wristTemperatureC {
                            Datum(title: "Wrist temperature · absolute", value: number(temperature, digits: 1), unit: "°C")
                        }
                        if let heartRate = sleep.overnightHeartRate {
                            Datum(title: "Night heart rate", value: number(heartRate), unit: "bpm")
                        }
                        if let breathing = sleep.overnightRespiratoryRate {
                            Datum(title: "Respiratory rate", value: number(breathing, digits: 1), unit: "/min")
                        }
                        if let oxygen = sleep.overnightOxygenPercent {
                            Datum(title: "Blood oxygen", value: number(oxygen, digits: 1), unit: "%")
                        }
                        if let hrv = sleep.overnightHRVMilliseconds {
                            Datum(title: "Night HRV SDNN", value: number(hrv), unit: "ms")
                        }
                    }
                    if sleep.sourceCount > 0 {
                        Text("\(sleep.sourceCount) sleep records considered")
                            .font(.caption2)
                            .foregroundStyle(BoazPalette.muted)
                    }
                } else {
                    unavailable("No readable sleep session yet")
                }
            }
        }
    }

    @ViewBuilder private func stages(_ sleep: SleepDisplay) -> some View {
        Datum(title: "Deep", value: duration(sleep.deepMinutes), unit: "")
        Datum(title: "Core", value: duration(sleep.coreMinutes), unit: "")
        Datum(title: "REM", value: duration(sleep.remMinutes), unit: "")
        Datum(title: "Awake", value: duration(sleep.awakeMinutes), unit: "")
    }
}

struct FitnessSectionView: View {
    let fitness: FitnessDisplay?

    var body: some View {
        BoazCard {
            VStack(alignment: .leading, spacing: 18) {
                HStack {
                    SectionEyebrow(text: "Fitness / activity")
                    Spacer()
                    if let day = fitness?.day {
                        Text(day, format: .dateTime.month(.abbreviated).day())
                            .font(.caption)
                            .foregroundStyle(BoazPalette.muted)
                    }
                }
                if let fitness {
                    ringRow("MOVE", value: fitness.moveKcal, goal: fitness.moveGoalKcal, unit: "kcal", color: BoazPalette.success)
                    ringRow("EXERCISE", value: fitness.exerciseMinutes, goal: fitness.exerciseGoalMinutes, unit: "min", color: BoazPalette.amber)
                    ringRow("STAND", value: fitness.standHours, goal: fitness.standGoalHours, unit: "h", color: .cyan)
                    HStack(spacing: 8) {
                        Datum(title: "Steps", value: number(fitness.steps), unit: "")
                        Datum(title: "Flights climbed", value: number(fitness.flights), unit: "")
                    }
                    if !fitness.recentWorkouts.isEmpty {
                        Rectangle().fill(BoazPalette.border).frame(height: 0.5)
                        Text("RECENT WORKOUTS")
                            .font(.system(size: 10, weight: .semibold))
                            .tracking(1.4)
                            .foregroundStyle(BoazPalette.muted)
                        ForEach(fitness.recentWorkouts.prefix(5)) { workout in
                            workoutRow(workout)
                        }
                    }
                } else {
                    unavailable("No readable activity summary yet")
                }
            }
        }
    }

    private func ringRow(_ label: String, value: Double?, goal: Double?, unit: String, color: Color) -> some View {
        VStack(alignment: .leading, spacing: 7) {
            HStack(alignment: .firstTextBaseline) {
                Text(label)
                    .font(.system(size: 10, weight: .bold))
                    .tracking(1.2)
                    .foregroundStyle(BoazPalette.secondary)
                Spacer()
                Text("\(number(value)) \(unit)")
                    .font(.system(size: 15, weight: .semibold, design: .rounded))
                    .monospacedDigit()
                if let goal, goal > 0 {
                    Text("/ \(number(goal))")
                        .font(.caption)
                        .monospacedDigit()
                        .foregroundStyle(BoazPalette.muted)
                }
            }
            if let value, let goal, goal > 0 {
                GeometryReader { geometry in
                    ZStack(alignment: .leading) {
                        Capsule().fill(BoazPalette.inset)
                        Capsule().fill(color).frame(width: geometry.size.width * min(1, max(0, value / goal)))
                    }
                }
                .frame(height: 5)
                .accessibilityLabel("\(label) progress")
                .accessibilityValue("\(number(value)) of \(number(goal)) \(unit)")
            } else {
                Text(goal == nil ? "Goal unavailable" : "No readable value")
                    .font(.caption2)
                    .foregroundStyle(BoazPalette.muted)
            }
        }
    }

    private func workoutRow(_ workout: WorkoutDisplay) -> some View {
        HStack(alignment: .top, spacing: 12) {
            Image(systemName: "figure.run")
                .font(.system(size: 18))
                .foregroundStyle(BoazPalette.success)
                .frame(width: 32, height: 32)
                .background(BoazPalette.inset, in: RoundedRectangle(cornerRadius: 8))
            VStack(alignment: .leading, spacing: 4) {
                Text(workout.title).font(.subheadline.weight(.medium))
                Text(workout.startedAt, format: .dateTime.month(.abbreviated).day().hour().minute())
                    .font(.caption2).foregroundStyle(BoazPalette.muted)
            }
            Spacer(minLength: 4)
            VStack(alignment: .trailing, spacing: 4) {
                Text("\(number(workout.durationMinutes)) min")
                    .font(.subheadline.weight(.semibold)).monospacedDigit()
                if let kcal = workout.energyKcal {
                    Text("\(number(kcal)) kcal")
                        .font(.caption2).foregroundStyle(BoazPalette.secondary)
                }
                if let distance = workout.distance?.value {
                    Text("\(number(distance)) m")
                        .font(.caption2).foregroundStyle(BoazPalette.secondary)
                }
                if let heartRate = workout.heartRate {
                    Text("\(heartRate.formattedValue) bpm avg · \(heartRate.detail ?? "range unavailable")")
                        .font(.caption2).foregroundStyle(BoazPalette.secondary)
                    Text("\(heartRate.sampleCount) associated heart-rate readings")
                        .font(.caption2).foregroundStyle(BoazPalette.muted)
                }
                if workout.eventCount > 0 {
                    Text("\(workout.eventCount) workout events")
                        .font(.caption2).foregroundStyle(BoazPalette.secondary)
                }
            }
        }
        .accessibilityElement(children: .combine)
    }
}

struct VitalsSectionView: View {
    let vitals: VitalsDisplay?
    @Environment(\.dynamicTypeSize) private var typeSize

    var body: some View {
        BoazCard {
            VStack(alignment: .leading, spacing: 16) {
                SectionEyebrow(text: "Vitals / latest readable values")
                if let vitals {
                    let readings = items(vitals)
                    if typeSize.isAccessibilitySize {
                        VStack(spacing: 8) { ForEach(readings) { reading in tile(reading) } }
                    } else {
                        LazyVGrid(columns: [.init(.flexible(), spacing: 8), .init(.flexible(), spacing: 8)], spacing: 8) {
                            ForEach(readings) { reading in tile(reading) }
                        }
                    }
                } else {
                    unavailable("No readable vital measurements yet")
                }
            }
        }
    }

    private struct Item: Identifiable {
        let id: String
        let title: String
        let value: Double?
        let unit: String
        let digits: Int
        let observedAt: Date?
        let sampleCount: Int?
    }

    private func items(_ vitals: VitalsDisplay) -> [Item] {
        let definitions: [(String, String, Double?, String, Int)] = [
            ("heartRate", "Heart rate", vitals.heartRate, "bpm", 0),
            ("restingHeartRate", "Resting heart", vitals.restingHeartRate, "bpm", 0),
            ("oxygenPercent", "Blood oxygen", vitals.oxygenPercent, "%", 1),
            ("weightKg", "Weight", vitals.weightKg, "kg", 1),
            ("hrvMilliseconds", "HRV SDNN", vitals.hrvMilliseconds, "ms", 0),
            ("systolicMMHg", "Systolic", vitals.systolicMMHg, "mmHg", 0),
            ("diastolicMMHg", "Diastolic", vitals.diastolicMMHg, "mmHg", 0),
            ("bodyFatPercent", "Body fat", vitals.bodyFatPercent, "%", 1),
            ("bmi", "BMI", vitals.bmi, "", 1)
        ]
        return definitions.map { id, title, value, unit, digits in
            Item(id: id, title: title, value: value, unit: unit, digits: digits,
                 observedAt: vitals.timestamps[id], sampleCount: vitals.sampleCounts[id])
        }
    }

    private func tile(_ item: Item) -> some View {
        let footnote: String = {
            if let date = item.observedAt { return date.formatted(date: .abbreviated, time: .shortened) }
            if let count = item.sampleCount { return "\(count) sample\(count == 1 ? "" : "s")" }
            return "Time unavailable"
        }()
        return Datum(title: item.title, value: number(item.value, digits: item.digits), unit: item.unit, footnote: footnote)
    }
}

private func unavailable(_ message: String) -> some View {
    VStack(alignment: .leading, spacing: 8) {
        Text("—").font(.system(size: 36, weight: .light)).foregroundStyle(.white)
        Text(message).font(.subheadline).foregroundStyle(BoazPalette.secondary)
        Text("Health may contain no records or may limit read access; the app cannot distinguish these cases.")
            .font(.caption).foregroundStyle(BoazPalette.muted)
            .fixedSize(horizontal: false, vertical: true)
    }
    .frame(maxWidth: .infinity, alignment: .leading)
}
