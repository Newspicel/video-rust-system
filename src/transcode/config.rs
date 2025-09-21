use std::env;

#[derive(Clone, Copy, Debug)]
pub struct EncodeParams {
    pub crf: u8,
    pub cpu_used: u8,
    pub(crate) encoder: Option<EncoderKind>,
}

impl EncodeParams {
    pub fn sanitized(self) -> Self {
        Self {
            crf: self.crf.clamp(0, 63),
            cpu_used: self.cpu_used.clamp(0, 8),
            encoder: self.encoder,
        }
    }

    pub(crate) fn preferred_encoder(&self) -> Option<EncoderKind> {
        self.encoder
    }
}

impl Default for EncodeParams {
    fn default() -> Self {
        Self {
            crf: 24,
            cpu_used: 4,
            encoder: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum EncoderKind {
    VideoToolboxAv1,
    NvencAv1,
    QsvAv1,
    VaapiAv1,
    SoftwareAv1,
}

fn encoder_from_env() -> Option<EncoderKind> {
    env::var("VIDEO_SERVER_ENCODER").ok().and_then(|value| {
        match value.to_ascii_lowercase().as_str() {
            "videotoolbox" | "vt" => Some(EncoderKind::VideoToolboxAv1),
            "nvenc" | "cuda" => Some(EncoderKind::NvencAv1),
            "qsv" | "quicksync" => Some(EncoderKind::QsvAv1),
            "vaapi" => Some(EncoderKind::VaapiAv1),
            "software" | "cpu" => Some(EncoderKind::SoftwareAv1),
            _ => None,
        }
    })
}

pub(crate) fn encoder_candidates(explicit: Option<EncoderKind>) -> Vec<EncoderKind> {
    let mut order = Vec::new();
    if let Some(kind) = explicit.or_else(encoder_from_env) {
        order.push(kind);
    } else {
        #[cfg(target_os = "macos")]
        {
            order.push(EncoderKind::VideoToolboxAv1);
        }
        #[cfg(target_os = "windows")]
        {
            order.push(EncoderKind::NvencAv1);
            order.push(EncoderKind::QsvAv1);
        }
        #[cfg(target_os = "linux")]
        {
            order.push(EncoderKind::VaapiAv1);
            order.push(EncoderKind::NvencAv1);
        }
    }
    order.push(EncoderKind::SoftwareAv1);
    order.sort_unstable();
    order.dedup();
    order
}
