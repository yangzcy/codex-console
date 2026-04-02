# Git 分支工作流

## 目的

当前仓库采用简洁的双分支开发方式：

- `main`：稳定、可部署、可发布的正式分支
- `dev`：日常开发分支

平时所有开发都先提交到 `dev`，远程 `main` 不接受直接推送，只通过远程 `dev -> main` 的 Pull Request 合并。

## 当前里程碑

当前重要节点已经打上标签：

- `milestone-2026-04-02`

这个标签对应的是切换到长期 `dev -> main` 工作流之前的仓库状态，可作为后续回滚或对照基线。

## 日常开发流程

开始开发前，先切到 `dev` 并同步远程：

```bash
cd /root/codex-console
git checkout dev
git pull userrepo dev
```

完成修改后提交到 `dev`：

```bash
git add .
git commit -m "feat: 你的改动说明"
git push userrepo dev
```

## 发布流程

当 `dev` 已经验证完成、可以发布时，不再从本地直接推送 `main`，而是发起远程 `dev -> main` 的 Pull Request：

```bash
cd /root/codex-console
git checkout dev
git pull userrepo dev
git push userrepo dev
```

然后在 GitHub 仓库页面发起：

```text
base: main
compare: dev
```

确认无误后在 GitHub 上完成合并。

本地如果需要同步最新正式代码，再执行：

```bash
cd /root/codex-console
git checkout main
git pull userrepo main
```

建议遵循以下原则：

- `main` 始终保持可部署状态
- 不要直接在 `main` 上开发
- `main` 只接收来自远程 `dev` 的合并
- 发布合并统一通过 GitHub PR 完成，历史记录更清晰

## 里程碑打标

在进行大改版、高风险调整或重要阶段收口前，建议先对当前稳定版本打标签：

```bash
git checkout main
git pull userrepo main
git tag -a milestone-YYYY-MM-DD -m "里程碑说明"
git push userrepo milestone-YYYY-MM-DD
```

## 回滚方式

如果 `main` 上线后的版本有问题，先定位最近一个可用节点：

```bash
git log --oneline --decorate --graph -n 30
git tag --list "milestone-*"
```

然后按情况选择：

- 回退出问题的合并提交
- 重新部署某个已确认正常的标签版本

除非你完全明确影响范围，否则不要在共享分支上直接使用 `git reset --hard`。

## 远程仓库配置

当前远程仓库的含义：

- `userrepo`：你的 GitHub 仓库
- `upstream`：原始上游仓库

常用同步方式：

```bash
git checkout main
git fetch upstream
git fetch userrepo
```

如果后续需要吸收上游更新，建议先在 `dev` 上处理和验证，不要直接在 `main` 上操作。

## 本地分支跟踪建议

建议本地分支按下面方式跟踪：

- 本地 `dev` 跟踪 `userrepo/dev`
- 本地 `main` 跟踪 `userrepo/main`

设置命令如下：

```bash
git branch --set-upstream-to=userrepo/dev dev
git branch --set-upstream-to=userrepo/main main
git checkout dev
```

这样以后日常开发时，默认停留在 `dev` 即可。

## GitHub 认证

这台服务器向 GitHub 推送代码时，需要可用的认证信息。最简单的方式是使用带仓库权限的 GitHub Personal Access Token。

### 方式一：HTTPS + PAT

1. 创建一个 GitHub Personal Access Token。
2. 建议最小权限如下：
   - Fine-grained Token：仅授权目标仓库，并赋予 `Contents: Read and write`
   - Classic Token：授予 `repo`
3. 在当前机器上启用凭据保存：

```bash
git config --global credential.helper store
```

4. 第一次 `git push` 时，Git 会要求输入：
   - Username：你的 GitHub 用户名
   - Password：填写 PAT，不是 GitHub 登录密码

5. 凭据会保存到：

```bash
/root/.git-credentials
```

后续再推送通常就不需要重复输入。

### 方式二：手动预写入凭据

如果你想一次性手动写入凭据：

```bash
printf 'https://YOUR_GITHUB_USERNAME:YOUR_GITHUB_PAT@github.com\n' > /root/.git-credentials
chmod 600 /root/.git-credentials
git config --global credential.helper store
```

把下面两项替换成你自己的信息：

- `YOUR_GITHUB_USERNAME`
- `YOUR_GITHUB_PAT`

### 方式三：SSH

如果你更倾向于 SSH，可以把远程地址改成 SSH 形式，并配置部署密钥或个人 SSH Key：

```bash
git remote set-url userrepo git@github.com:yangzcy/codex-console.git
```

然后把这台机器对应的公钥添加到你的 GitHub 账号中。

## 建议的分支保护

建议在 GitHub 上对 `main` 开启保护：

- 禁止直接推送
- 要求通过 Pull Request 合并进入 `main`
- 将 `dev` 作为持续开发分支
- 对管理员同样生效，避免误操作绕过保护

即使是小型私有项目，不走完整 PR 流程，保护 `main` 也依然有价值。
