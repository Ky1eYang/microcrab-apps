//! Display-only failed attempts: durable references, or complete volatile fallback.
use microcrab_transport::{ModelObservation, ObservationState};
use serde_json::{Value, json};

#[cfg(test)]
const MAX_BYTES: usize = 64 * 1024;

#[derive(Default)]
pub(super) struct PartialOutput {
    call_id: String,
    epoch: u64,
    turn: Option<u64>,
    round: Option<u64>,
    text: String,
    reasoning: String,
    invalidated: bool,
    validated: bool,
    truncated: bool,
    archive: Option<(String, u64)>,
    observed_body: bool,
}
impl PartialOutput {
    pub(super) fn observe(&mut self, observation: &ModelObservation) {
        if matches!(observation.state, ObservationState::Started) {
            *self = Self {
                call_id: observation.call_id.clone(),
                epoch: observation.epoch,
                turn: observation.turn,
                round: observation.round,
                ..Self::default()
            };
            return;
        }
        if self.call_id != observation.call_id
            || self.epoch != observation.epoch
            || self.turn != observation.turn
            || self.round != observation.round
        {
            return;
        }
        match &observation.state {
            ObservationState::TextDelta { text } | ObservationState::ReasoningDelta { text } => {
                self.observed_body |= !text.is_empty();
                if matches!(observation.state, ObservationState::TextDelta { .. }) {
                    self.text.push_str(text);
                } else {
                    self.reasoning.push_str(text);
                }
            }
            ObservationState::Invalidated { .. } => self.invalidated = true,
            ObservationState::Validated { .. } => self.validated = true,
            _ => {}
        }
    }
    pub(super) fn observe_recorded(
        &mut self,
        observation: &ModelObservation,
        attempt: &str,
        sequence: u64,
    ) {
        if matches!(observation.state, ObservationState::Started) {
            self.observe(observation);
        } else if self.call_id == observation.call_id
            && self.epoch == observation.epoch
            && self.turn == observation.turn
            && self.round == observation.round
        {
            match &observation.state {
                ObservationState::TextDelta { text }
                | ObservationState::ReasoningDelta { text } => {
                    self.observed_body |= !text.is_empty()
                }
                ObservationState::Invalidated { .. } => self.invalidated = true,
                ObservationState::Validated { .. } => self.validated = true,
                _ => {}
            }
        } else {
            return;
        }
        self.archive = Some((attempt.to_owned(), sequence));
    }
    #[cfg(test)]
    pub(super) fn failed(
        &self,
        epoch: u64,
        turn: Option<u64>,
        round: Option<u64>,
    ) -> Option<Value> {
        self.failed_or_rejected(epoch, turn, round, false)
    }
    pub(super) fn failed_or_rejected(
        &self,
        epoch: u64,
        turn: Option<u64>,
        round: Option<u64>,
        pre_effect: bool,
    ) -> Option<Value> {
        ((self.invalidated || (pre_effect && self.validated))
            && self.epoch == epoch
            && self.turn == turn
            && self.round == round
            && (self.observed_body || !self.text.is_empty() || !self.reasoning.is_empty()))
        .then(|| {
            if let Some((attempt, sequence)) = &self.archive {
                return json!({"call_id":self.call_id,"epoch":self.epoch,"turn":self.turn,"round":self.round,
                    "display_ref":{"attempt_id":attempt,"high_water":sequence},"truncated":false});
            }
            json!({
                "call_id": self.call_id, "epoch": self.epoch, "turn": self.turn,
                "round": self.round, "text": self.text, "reasoning": self.reasoning,
                "truncated": self.truncated,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn observation(call_id: &str, state: ObservationState) -> ModelObservation {
        ModelObservation {
            call_id: call_id.into(),
            epoch: 1,
            turn: Some(2),
            round: Some(3),
            subagent: None,
            state,
        }
    }
    #[test]
    fn failed_preview_is_exact_attempt_scoped_bounded_and_never_an_accepted_response() {
        let mut partial = PartialOutput::default();
        partial.observe(&observation("a", ObservationState::Started));
        partial.observe(&observation(
            "foreign",
            ObservationState::TextDelta {
                text: "foreign".into(),
            },
        ));
        partial.observe(&observation(
            "a",
            ObservationState::TextDelta {
                text: "未完".repeat(MAX_BYTES),
            },
        ));
        assert!(partial.failed(1, Some(2), Some(3)).is_none());
        partial.observe(&observation(
            "a",
            ObservationState::Invalidated {
                reason: "timed_out".into(),
            },
        ));
        let value = partial.failed(1, Some(2), Some(3)).unwrap();
        assert!(!value["truncated"].as_bool().unwrap());
        assert_eq!(value["text"], "未完".repeat(MAX_BYTES));
        assert!(!value["text"].as_str().unwrap().contains("foreign"));
        assert!(partial.failed(2, Some(2), Some(3)).is_none());
        partial.observe(&observation("b", ObservationState::Started));
        assert!(partial.failed(1, Some(2), Some(3)).is_none());
        partial.observe(&observation(
            "b",
            ObservationState::TextDelta {
                text: "accepted".into(),
            },
        ));
        partial.observe(&observation(
            "b",
            ObservationState::Validated {
                text: "accepted".into(),
            },
        ));
        assert!(partial.failed(1, Some(2), Some(3)).is_none());
        assert_eq!(
            partial
                .failed_or_rejected(1, Some(2), Some(3), true)
                .unwrap()["text"],
            "accepted"
        );
        assert!(
            partial
                .failed_or_rejected(2, Some(2), Some(3), true)
                .is_none()
        );
    }
}
