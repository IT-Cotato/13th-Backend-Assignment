# 과제 제출 방법

본 저장소의 과제는 각자 저장소를 **fork**한 뒤, 본인의 fork 저장소에서 작업하고 Pull Request로 제출합니다.

> 절대 원본 저장소의 `develop` 브랜치에서 직접 수정하지 않습니다.  
> 반드시 본 저장소를 fork한 뒤, 본인의 fork 저장소에서 작업하고 PR을 올려주세요.

---

## 1. 과제 진행 방식

각 주차 폴더는 다음과 같이 구성됩니다.

```text
Week1/
├── concept.md      # 개념 설명
├── assignment.md   # 과제 내용
└── solution.md     # 해설
```

진행 순서는 다음과 같습니다.

1. 해당 주차의 `concept.md`를 읽습니다.
2. `assignment.md`의 과제를 수행합니다.
3. 본 저장소를 본인의 GitHub 계정으로 fork합니다.
4. 본인의 fork 저장소를 로컬에 clone합니다.
5. 본인의 fork 저장소에서 개인 브랜치를 생성합니다.
6. 본인 제출 파일을 `submissions/WeekN/` 아래에 작성합니다.
7. 원본 저장소의 `develop` 브랜치로 Pull Request를 생성합니다.
8. 마감 후 `solution.md` 해설을 확인합니다.

---

## 2. 과제 제출 위치

제출 파일은 반드시 아래 경로에 작성합니다.

```text
submissions/WeekN/기수_이름.md
```

예시:

```text
submissions/Week1/13기_김기민.md
submissions/Week2/13기_김기민.md
```

Week1 과제는 아래 위치에 제출합니다.

```text
submissions/Week1/13기_김기민.md
```

Week2 과제는 아래 위치에 제출합니다.

```text
submissions/Week2/13기_김기민.md
```

> 절대 다른 사람의 제출 파일이나 다른 사람의 폴더에 과제를 올리지 않습니다.  
> 반드시 `submissions` 디렉토리 아래의 해당 주차 폴더에 제출합니다.

---

## 3. Fork 규칙

과제를 제출할 때는 먼저 원본 저장소를 본인의 GitHub 계정으로 fork합니다.

fork를 하면 본인의 GitHub 계정 아래에 같은 저장소가 복사됩니다.

예를 들어 원본 저장소가 아래와 같다면,

```text
backend-study/13th-Backend-Assignment
```

fork 후에는 본인의 계정 아래에 다음과 같은 저장소가 생성됩니다.

```text
본인계정/13th-Backend-Assignment
```

과제 작업은 반드시 본인의 fork 저장소에서 진행합니다.

> 원본 저장소에서 직접 파일을 수정하지 않습니다.  
> 원본 저장소에 직접 브랜치를 만들지 않습니다.  
> 본인의 fork 저장소에서 작업한 뒤, 원본 저장소로 PR을 보냅니다.

---

## 4. 브랜치 규칙

본인의 fork 저장소에서 새로운 개인 브랜치를 생성합니다.

브랜치 이름은 아래 형식을 사용합니다.

```text
submit/weekN/이름
```

예시:

```text
submit/week1/gimin
submit/week2/gimin
```

> 본인의 fork 저장소에서도 `develop` 브랜치에서 직접 작업하지 않습니다.  
> 반드시 개인 브랜치를 생성한 뒤, 그 브랜치에서 제출 파일을 작성합니다.

---

## 5. Pull Request 규칙

PR은 반드시 원본 저장소의 `develop` 브랜치로 보냅니다.

PR 제목은 아래 형식으로 작성합니다.

```text
[WeekN] 이름 과제 제출
```

예시:

```text
[Week1] 김기민 과제 제출
[Week2] 김기민 과제 제출
```

PR 생성 시 아래 내용을 확인합니다.

```text
base repository: 원본 저장소
base: develop

head repository: 본인의 fork 저장소
compare: submit/weekN/이름
```

즉, 본인의 fork 저장소에서 작업한 브랜치를 원본 저장소의 `develop` 브랜치로 보내는 방식입니다.

---

## 6. Git 제출 흐름

먼저 원본 저장소를 GitHub에서 fork합니다.

그다음 본인의 fork 저장소를 clone합니다.

```bash
git clone https://github.com/본인계정/13th-Backend-Assignment.git
cd 13th-Backend-Assignment
```

원본 저장소를 `upstream`으로 등록합니다.

```bash
git remote add upstream https://github.com/원본계정/13th-Backend-Assignment.git
```

원본 저장소의 최신 `develop` 브랜치를 가져옵니다.

```bash
git fetch upstream
git checkout develop
git pull upstream develop
```

개인 브랜치를 생성합니다.

```bash
git checkout -b submit/week1/gimin
```

과제 제출 파일을 작성합니다.

```text
submissions/Week1/13기_김기민.md
```

작성 후 커밋합니다.

```bash
git add submissions/Week1/13기_김기민.md
git commit -m "docs: Week1 과제 제출 - 김기민"
```

본인의 fork 저장소로 push합니다.

```bash
git push origin submit/week1/gimin
```

이후 GitHub에서 Pull Request를 생성합니다.

PR 대상은 반드시 아래와 같이 설정합니다.

```text
base repository: 원본 저장소
base: develop

head repository: 본인의 fork 저장소
compare: submit/week1/gimin
```

---

## 7. 다음 주차 과제 제출 흐름

Week2 이후에도 항상 원본 저장소의 최신 `develop` 브랜치를 먼저 가져온 뒤 시작합니다.

```bash
git fetch upstream
git checkout develop
git pull upstream develop
```

새로운 주차 브랜치를 생성합니다.

```bash
git checkout -b submit/week2/gimin
```

과제 제출 파일을 작성합니다.

```text
submissions/Week2/13기_김기민.md
```

커밋 후 push합니다.

```bash
git add submissions/Week2/13기_김기민.md
git commit -m "docs: Week2 과제 제출 - 김기민"
git push origin submit/week2/gimin
```

이후 GitHub에서 원본 저장소의 `develop` 브랜치로 PR을 생성합니다.

---

## 8. 제출 시 주의사항

- 원본 저장소에서 직접 수정하지 않습니다.
- 원본 저장소에 직접 브랜치를 만들지 않습니다.
- 반드시 본인의 GitHub 계정으로 fork한 뒤 작업합니다.
- 본인의 fork 저장소에서도 `develop` 브랜치에서 직접 작업하지 않습니다.
- 제출 파일은 반드시 `submissions/WeekN/` 아래에 작성합니다.
- 파일명은 `기수_이름.md` 형식을 사용합니다.
- PR 대상 브랜치는 반드시 원본 저장소의 `develop`입니다.
- 다른 사람의 제출 파일을 수정하지 않습니다.
- 다른 사람의 폴더나 잘못된 주차 폴더에 제출하지 않습니다.
- 본인이 제출하는 주차에 맞는 폴더에만 파일을 올립니다.

