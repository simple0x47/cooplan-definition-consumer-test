use cooplan_definitions_lib::definition::Definition;

pub struct InputState {
    available: bool,
    definition: Option<Definition>,
}

impl InputState {
    pub fn new(available: bool, definition: Option<Definition>) -> InputState {
        InputState {
            available,
            definition,
        }
    }

    pub fn available(&self) -> bool {
        self.available
    }

    pub fn definition(&self) -> Option<Definition> {
        self.definition.clone()
    }
}
