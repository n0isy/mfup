import { expect, test } from "@playwright/test";
import fs from "node:fs/promises";
import path from "node:path";
const file = (name: string, content: string) => ({
  name,
  mimeType: "text/plain",
  buffer: Buffer.from(content),
});
test.beforeEach(async ({ page }) => {
  await page.goto("/");
  await expect(page.getByTestId("backend")).toHaveText(
    process.env.MFUP_BACKEND ?? "node",
  );
});
test("anonymous cookie persists and scopes expose only this user's files", async ({
  page,
  context,
  browser,
}) => {
  const uid = await page.getByTestId("user-id").textContent();
  await page
    .getByLabel("Files workspace", { exact: true })
    .setInputFiles(file("own.txt", "workspace"));
  await page
    .getByLabel("Files scratch", { exact: true })
    .setInputFiles(file("own.txt", "scratch"));
  for (const scope of ["workspace", "scratch"]) {
    await expect(
      page.getByTestId(`zone-${scope}`).getByTestId("status"),
    ).toHaveText("Upload complete");
    await expect(
      page.getByTestId(`zone-${scope}`).getByTestId("listing"),
    ).toContainText("own.txt");
    expect(
      await (
        await context.request.get(`/api/file/${scope}?path=own.txt`)
      ).text(),
    ).toBe(scope);
  }
  await page.reload();
  await expect(page.getByTestId("user-id")).toHaveText(uid!);
  await expect(
    page.getByTestId("zone-workspace").getByTestId("listing"),
  ).toContainText("own.txt");
  const other = await browser.newContext({ baseURL: "http://127.0.0.1:20067" });
  try {
    const second = await other.newPage();
    await second.goto("/");
    await expect(second.getByTestId("user-id")).not.toHaveText(uid!);
    await expect(
      second.getByTestId("zone-workspace").getByTestId("listing"),
    ).toContainText("Empty for now");
    expect(
      (await other.request.get("/api/file/workspace?path=own.txt")).status(),
    ).toBe(404);
  } finally {
    await other.close();
  }
});
test("session approval, cancel and reload resume use the same scoped destination", async ({
  page,
  context,
}) => {
  const zone = page.getByTestId("zone-uploads"),
    input = page.getByLabel("Files uploads", { exact: true });
  await input.setInputFiles(file("same.txt", "old"));
  await expect(zone.getByTestId("status")).toHaveText("Upload complete");
  await expect(zone.getByTestId("listing")).toContainText("same.txt");
  await input.setInputFiles(file("same.txt", "cancelled"));
  await zone
    .getByRole("button", { name: "Cancel upload", exact: true })
    .click();
  await expect(zone.getByTestId("status")).toHaveText("Upload cancelled");
  expect(
    await (await context.request.get("/api/file/uploads?path=same.txt")).text(),
  ).toBe("old");
  await input.setInputFiles(file("same.txt", "new"));
  await expect(
    zone.getByRole("button", { name: "Allow overwrite", exact: true }),
  ).toBeVisible();
  await expect(zone.getByTestId("status")).toHaveText(
    "Your approval is needed",
  );
  await page.reload();
  await expect(zone).toContainText("An unfinished upload is saved");
  await input.setInputFiles(file("same.txt", "new"));
  await zone
    .getByRole("button", { name: "Allow overwrite", exact: true })
    .click();
  await expect(zone.getByTestId("status")).toHaveText("Upload complete");
  expect(
    await (await context.request.get("/api/file/uploads?path=same.txt")).text(),
  ).toBe("new");
});
test("folder picker preserves the tree and published files can be browsed and downloaded", async ({
  page,
  context,
}, info) => {
  const folder = info.outputPath("folder");
  await fs.mkdir(path.join(folder, "nested"), { recursive: true });
  await fs.writeFile(path.join(folder, "nested", "leaf.txt"), "nested content");
  const zone = page.getByTestId("zone-workspace");
  await page
    .getByLabel("Folder workspace", { exact: true })
    .setInputFiles(folder);
  await expect(zone.getByTestId("status")).toHaveText("Upload complete");
  await zone.getByRole("button", { name: "▸ folder", exact: true }).click();
  await zone.getByRole("button", { name: "▸ nested", exact: true }).click();
  await expect(
    zone.getByRole("link", { name: "leaf.txt", exact: true }),
  ).toBeVisible();
  expect(
    await (
      await context.request.get(
        "/api/file/workspace?path=folder/nested/leaf.txt",
      )
    ).text(),
  ).toBe("nested content");
});

test("published listings page through files without accumulating the directory", async ({
  page,
}) => {
  const zone = page.getByTestId("zone-uploads");
  await page
    .getByLabel("Files uploads", { exact: true })
    .setInputFiles(
      Array.from({ length: 270 }, (_, i) =>
        file(`f${String(i).padStart(3, "0")}.txt`, "page"),
      ),
    );
  await expect(zone.getByTestId("status")).toHaveText("Upload complete");
  await expect(zone.getByTestId("listing").locator("li")).toHaveCount(256);
  await zone.getByRole("button", { name: "Next page" }).click();
  await expect(zone.getByTestId("listing").locator("li")).toHaveCount(14);
  await expect(zone.getByTestId("listing")).toContainText("f269.txt");
  await zone.getByRole("button", { name: "First page" }).click();
  await expect(zone.getByTestId("listing").locator("li")).toHaveCount(256);
});
