// Unified-System-Subsystem-Temporal-Consistency-Validator.ts
// SAIA-Class 300 — deterministic subsystem temporal consistency validator.

export interface TemporalEvent {
  eventId: string;
  timestampIso: string;
  eventHash: string;        // hash representing event state
}

export interface TemporalConsistencyPacket {
  packetId: string;
  engineId: string;
  subsystemId: string;
  events: TemporalEvent[];
  driftToleranceMs: number; // allowable timestamp drift
  timestampIso: string;
}

export type TemporalStatus =
  | "TEMPORAL_CONSISTENT"
  | "TEMPORAL_DRIFT"
  | "TEMPORAL_REVERSAL"
  | "TIMESTAMP_CORRUPTION"
  | "INVALID_SEQUENCE"
  | "TIMESTAMP_ERROR";

export interface TemporalRuling {
  rulingId: string;
  packetId: string;
  status: TemporalStatus;
  details: string;
  issuedAtIso: string;
  issuedByEngineId: string;
}

export interface TemporalConsistencyConfig {
  engineId: string;
  reversalThreshold: number; // number of reversals allowed before hard failure
}

export class UnifiedSystemSubsystemTemporalConsistencyValidator {
  private readonly config: TemporalConsistencyConfig;

  constructor(config: TemporalConsistencyConfig) {
    this.config = config;
  }

  public evaluate(packet: TemporalConsistencyPacket): TemporalRuling {
    const status = this.resolveStatus(packet);

    return {
      rulingId: this.generateRulingId(packet),
      packetId: packet.packetId,
      status,
      details: this.describe(status),
      issuedAtIso: new Date().toISOString(),
      issuedByEngineId: this.config.engineId,
    };
  }

  private resolveStatus(packet: TemporalConsistencyPacket): TemporalStatus {
    if (!packet.timestampIso) return "TIMESTAMP_ERROR";

    if (!packet.events || packet.events.length < 2) {
      return "INVALID_SEQUENCE";
    }

    let reversalCount = 0;
    let driftDetected = false;

    for (let i = 1; i < packet.events.length; i++) {
      const prev = packet.events[i - 1];
      const curr = packet.events[i];

      const prevTime = new Date(prev.timestampIso).getTime();
      const currTime = new Date(curr.timestampIso).getTime();

      if (isNaN(prevTime) || isNaN(currTime)) {
        return "TIMESTAMP_CORRUPTION";
      }

      // Temporal reversal detection
      if (currTime < prevTime) {
        reversalCount++;
      }

      // Drift detection
      const drift = Math.abs(currTime - prevTime);
      if (drift > packet.driftToleranceMs) {
        driftDetected = true;
      }
    }

    if (reversalCount > this.config.reversalThreshold) {
      return "TEMPORAL_REVERSAL";
    }

    if (driftDetected) {
      return "TEMPORAL_DRIFT";
    }

    return "TEMPORAL_CONSISTENT";
  }

  private describe(status: TemporalStatus): string {
    switch (status) {
      case "TEMPORAL_CONSISTENT":
        return "Temporal sequence consistent; monotonic time progression validated.";
      case "TEMPORAL_DRIFT":
        return "Temporal drift detected; timestamps diverging beyond tolerance.";
      case "TEMPORAL_REVERSAL":
        return "Temporal reversal detected; non‑monotonic event sequence.";
      case "TIMESTAMP_CORRUPTION":
        return "Timestamp corruption detected; invalid or unreadable timestamps.";
      case "INVALID_SEQUENCE":
        return "Temporal event sequence missing or insufficient.";
      case "TIMESTAMP_ERROR":
        return "Missing or invalid packet timestamp.";
    }
  }

  private generateRulingId(packet: TemporalConsistencyPacket): string {
    return `TEMP-${this.config.engineId}-${packet.packetId}-${Date.now()}`;
  }
}

export const DEFAULT_TEMPORAL_CONSISTENCY_CONFIG: TemporalConsistencyConfig = {
  engineId: "Unified-System-Subsystem-Temporal-Consistency-Validator-Class-300",
  reversalThreshold: 0,
};
