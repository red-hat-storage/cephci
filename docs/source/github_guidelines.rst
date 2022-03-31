===========================
Getting Started with GitHub
===========================

If you're concerned that you haven't done it right, don't worry. Submit your pull request, and we'll help you get the details sorted out.

Getting the Code
=================

We recommend forking the project first, and then cloning the fork.

    ```bash
    git@github.com:red-hat-storage/cephci.git
    ```

This will give you a remote repository named `origin` that points to your own copy of the project.

In addition to this, we recommend that you add the original repository as a secondary remote.

    ```bash
    git remote add upstream https://github.com/red-hat-storage/cephci.git
    ```

The names `origin` and `upstream` are pretty standard, so it's worth getting used to the names in your own work.

Branches
=========

When working on your fork it tends to make things easier if you never touch the master branch.

The basic workflow becomes:

    * check out the master branch
    * pull from `upstream` to make sure everything is up to date
    * push to `origin` so you have the latest code in your fork
    * check out a branch
    * make the changes, commit them
    * rebase onto the upstream master (and resolve any conflicts)
    * push your branch up to `origin`
    * submit a pull request

If you're asked to tweak your work, you can keep pushing it to the branch, and it automatically updates the pull request with the new commits.

Examples
=========

Imagine that you're submitting a new problem called "refactor_utils" to the master.

Here's a fairly typical set of commits that you might end up making:

    ```bash
    437a0e1 added initial workflow design
    1f7d4ba refactored exec_command
    cf21747 update suite yaml
    be4c410 rename example file
    bb89a77 update config
    ```

All of these commits are about a single purpose. They
should be a single commit. Once you're done, you should squash everything into one commit,
and rename it:

    ```bash
    f4314e5 added initial workflow design
    ```

Resetting `master`
==================

If you've already made changes on your master so that it has diverged from the
upstream you can reset it.

First create a backup of your branch so that you can find any changes.

    ```bash
    git checkout master
    git checkout -b backup
    git checkout master
    ```

Next, fetch the most recent changes from the upstream repository and reset master to it.

    ```bash
    git fetch upstream
    git reset --hard upstream/master
    ```

If you do a git log at this point you'll see that you have exactly the
commits that are in the upstream repository. Your commits aren't gone, they're
just not in master anymore.

To put this on your GitHub fork, you'll probably need to use the `--force` flag:

    ```bash
    git push --force origin master
    ```

Squashing
=========

Squashing commits into a single commit is particularly useful when the change
happened in lots of little steps, but it really is one
change.

There are a number of ways to accomplish this, and many people like to use an
[interactive rebase](#interactive-rebase), but it can be tricky if you haven't set git up to open
your favorite editor.

An easier way to do this is to un-commit everything, putting it back into the
staging area, and then committing it again.

Using the example from above, we have 5 commits that should be squashed into one.

    ```bash
    437a0e1 added initial workflow design
    1f7d4ba refactored exec_command
    cf21747 update suite yaml
    be4c410 rename example file
    bb89a77 update config
    ```

To un-commit them, use the following incantation:

    ```bash
    $ git reset --soft HEAD^^^^^
    ```

Notice that there are 5 carets, one for each commit that you want to
un-commit. You could also say:

    ```bash
    $ git reset --soft HEAD~5
    ```

If you do a `git status` now, you'll see all the changed files, and they're
staged and ready to commit. If you do a `git log`, you'll see that the most
recent commit is from someone else.

Next, commit, as usual:

    ```bash
    $ git commit -m "added initial workflow design"
    ```

Now if you do a `git status` you may get a warning saying that your origin and
your branch have diverged. This is completely normal, since the origin has 5
commits and you have 1 (different) one.

The next step is to force push this up to your branch, which will
automatically update the pull request, replacing the old commits with the new
one.

    ```bash
    $ git push --force origin <branchname>
    ```

Rebasing
========

You'll often be asked to rebase your branch before we merge a pull request as RHCS likes to keep a linear project commit history. This is accomplished with git rebase. It takes the current branch and places all the commits at the front of the branch that you're rebasing with.

For example, rebasing the current branch on top of upstream/master:
    ```
    git rebase upstream/master
    ```
Project commit history:
```
                       -- current branch --
                      /
--- master branch ----
```

Interactive Rebase
==================
The rebase command has an option called `-i, --interactive` which will open an editor with a list of the commits which are about to be changed. This list accepts commands, allowing the user to edit the list before initiating the rebase action.

Using the example from above, we have 5 commits that should be squashed into one.

    ```bash
    437a0e1 added initial workflow design
    1f7d4ba refactored exec_command
    cf21747 update suite yaml
    be4c410 rename example file
    bb89a77 update config
    ```

To interactively rebase, use the following:

    ```bash
    $ git rebase -i HEAD~5
    ```
This will bring up an editor with the following information:

    ```bash
    pick 437a0e1 added initial workflow design
    pick 1f7d4ba refactored exec_command
    pick cf21747 update suite yaml
    pick be4c410 rename example file
    pick bb89a77 update config

    #
    # Commands:
    #  p, pick = use commit
    #  r, reword = use commit, but edit the commit message
    #  e, edit = use commit, but stop for amending
    #  s, squash = use commit, but meld into previous commit
    #  f, fixup = like "squash", but discard this commit's log message
    #  x, exec = run command (the rest of the line) using shell
    #
    # These lines can be re-ordered; they are executed from top to bottom.
    #
    # If you remove a line here THAT COMMIT WILL BE LOST.
    #
    # However, if you remove everything, the rebase will be aborted.
    #
    # Note that empty commits are commented out
    ```

By choosing the `reword` command for the top commit and choosing the `fixup` command for the remaining commits, you will be able to squash the commits into one commit and provide a descriptive summary of the entire change

    ```bash
    reword 437a0e1 added initial workflow design
    fixup 1f7d4ba refactored exec_command
    fixup cf21747 update suite yaml
    fixup be4c410 rename example file
    fixup bb89a77 update config
    ```

[Further Reading](https://www.atlassian.com/git/tutorials/merging-vs-rebasing)
