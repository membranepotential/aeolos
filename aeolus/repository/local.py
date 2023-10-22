from aeolus import Step, Repository


class Local(Repository):
    def run(self, step: Step):
        self.command(step.format_command(), step=step)
