Github stuff

How to Pull develop
git pull origin develop (When you are on the develop branch)

How branch from develop
git checkout -b <branch-name>

How to add changes
git add <files>

How to commit 
git commit -m "commit msg here"

How to push to develop
git push origin <current branch name>

after you push and want to create a pull request to merge your changes into develop,
go to github.com and create PR, then mergeit in and delete the branch you merged into develop from.

git stash 
saves local changes to a stash and remove them, returns local branch to last commit on the branch

git stash apply 
applies the last stashed changes onto current branch



Using SSH:

ssh-keygen
Generates the ssh key

ssh-copy-id name@localhost
Use your account for ssh

ssh-add
Add current to key

If you get ambiguous output redirect, try deleting .ssh folder in your home folder, then do these commands. Make sure "ssh localhost" works without a password.
