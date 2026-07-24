We require all contributors to include a [`Signed-off-by`
line](https://github.com/apps/dco) to all commits in pull requests. This
matches what the Ceph upstream project does. For example:

```
Signed-off-by: Ken Dreyer <kdreyer@redhat.com>
```

## Manually adding Signed-off-by lines

You can manually add your `Signed-off-by` line to each commit log with the `-s`
flag (like `git commit -s`).

## Automatically adding Signed-off-by lines

Alternatively, you can set up a local Git hook script to add this
`Signed-off-by` line to all commits automatically.

1. Clone the cephci repository:

   ```
   git clone https://github.com/red-hat-storage/cephci
   ```

2. `cd` into the Git clone:

   ```
   cd cephci/
   ```

3. `cd` into the `.git/hooks/` directory:

   ```
   cd .git/hooks/
   ```

3. Create a new `commit-msg` shell script:

   ```
   vi commit-msg
   ```

4. Copy this script into this `commit-msg` file:

   ```
   #!/bin/sh

   NAME=$(git config user.name)
   EMAIL=$(git config user.email)

   if [ -z "$NAME" ]; then
       echo "empty git config user.name"
       exit 1
   fi

   if [ -z "$EMAIL" ]; then
       echo "empty git config user.email"
       exit 1
   fi

   git interpret-trailers --if-exists doNothing --trailer \
       "Signed-off-by: $NAME <$EMAIL>" \
       --in-place "$1"
   ```

5. Make that `commit-msg` file executable:

   ```
   chmod +x commit-msg
   ```

6. Symlink this script to the `prepare-commit-msg` filename. Git will use both
   hooks.

   ```
   ln -s commit-msg prepare-commit-msg
   ```

Once you have completed these steps, Git will automatically add the
`Signed-off-by:` line to all of your commits.

## Rewriting an existing commit to add a Signed-off-by line

If another developer asks you to add a `Signed-off-by` line to your in-progress
pull request, you can amend your commit to add it.

1. Check out the branch that has your work-in-progress commit:

   ```
   git checkout <branchname>
   ```

2. Ensure that the commit at the tip of this branch (your "HEAD" commit) is the
   one that lacks `Signed-off-by` *and* it's the one you want to edit:

   ```
   git show
   ```

3. Amend the commit. This will open up your `$EDITOR` (eg. vim). If you set up
   the `commit-msg` and `prepare-commit-msg` Git hooks (above), then you will
   see a new `Signed-off-by` line present now in the commit log. Save and quit
   your editor.

   ```
   git commit --amend
   ```

4. Force-push your amended commit to your remote branch. (We have to force-push
   because we're technically rewriting history any time we use `--amend`)

   ```
   git push -f
   ```

If you have an open pull request, you should see the "DCO bot" show a green
checkmark for your pull request.
