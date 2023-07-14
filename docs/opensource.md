# Open Source

## How to release the project to maven central?
- Make sure you are using personal github account instead of enterprise one.
- Update the version in all POM files.
    - They look like `<version>#.#.#</version>`
- PR the change and merge into mainline.
- `git push origin tag <version>` to create the tag
- `git tag <version>` to tag your change
- `git push` to update the remote repository
- A new workflow should be triggered
    - The publish-to-maven workflow can be found here: https://github.com/pinterest/memq/actions. 
    - It's automatically triggered by a push with tag. Script: https://github.com/pinterest/memq/blob/main/.github/workflows/publish_to_maven_central.yaml
- Find someone to approve the workflow, then the workflow should be able to process.
- After 2-6 hours, the new version should be able to load from maven central. 