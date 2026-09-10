import { expect, test } from "@playwright/test";
import { holdReceipts } from "../helpers/hold-receipt";
const files = (body: string) =>
  Array.from({ length: 12 }, (_, i) => ({
    name: `existing-${i}.txt`,
    mimeType: "text/plain",
    buffer: Buffer.from(body),
  }));
test.beforeEach(async ({ page }) => {
  await page.goto("/");
  await expect(page.getByTestId("backend")).toHaveText(
    process.env.MFUP_BACKEND ?? "node",
  );
});
for (const phase of ["during", "after"] as const)
  for (const decision of ["approve", "cancel"] as const) {
    test(`one session decision: ${decision} ${phase} upload`, async ({
      page,
      context,
    }) => {
      const zone = page.getByTestId("zone-uploads");
      const input = page.getByLabel("Files uploads", { exact: true });
      await input.setInputFiles(files("old"));
      await expect(zone.getByTestId("status")).toHaveText("Upload complete");
      const proxy =
        phase === "during"
          ? await holdReceipts("http://127.0.0.1:20067")
          : null;
      const release = () => proxy?.release();
      const grants: any[] = [];
      page.on("request", (request) => {
        if (request.url().endsWith("/properties"))
          grants.push(request.postDataJSON());
      });
      if (proxy)
        await page.route("**/mfup/sessions/*/batches/*", (route) => {
          const original = new URL(route.request().url());
          return route.continue({
            url: proxy.origin + original.pathname + original.search,
          });
        });
      await page.evaluate(() => {
        (window as any).maxPrompts = 0;
        new MutationObserver(() => {
          (window as any).maxPrompts = Math.max(
            (window as any).maxPrompts,
            document.querySelectorAll(
              '[data-testid="zone-uploads"] [data-testid="overwrite-prompt"]',
            ).length,
          );
        }).observe(document.body, { childList: true, subtree: true });
      });
      try {
        await input.setInputFiles([
          ...files("new"),
          {
            name: "unique.txt",
            mimeType: "text/plain",
            buffer: Buffer.from("unique"),
          },
        ]);
        const prompt = zone.getByTestId("overwrite-prompt");
        await expect(prompt).toHaveCount(1);
        await expect(prompt).not.toContainText("existing-");
        if (phase === "during")
          await expect(zone.getByTestId("status")).toHaveText("Uploading");
        else
          await expect(zone.getByTestId("status")).toHaveText(
            "Your approval is needed",
          );
        if (decision === "approve") {
          await prompt
            .getByRole("button", { name: "Allow overwrite", exact: true })
            .click();
          await expect(prompt).toHaveCount(0);
          release();
          await expect(zone.getByTestId("status")).toHaveText(
            "Upload complete",
          );
          expect(grants).toEqual([{ overwrite: true }]);
        } else {
          await prompt
            .getByRole("button", { name: "Cancel upload", exact: true })
            .click();
          await expect(zone.getByTestId("status")).toHaveText(
            "Upload cancelled",
          );
          release();
          expect(grants).toEqual([]);
        }
        await expect(zone.getByTestId("overwrite-prompt")).toHaveCount(0);
        await expect(zone.getByRole("alert")).toHaveCount(0);
        expect(await page.evaluate(() => (window as any).maxPrompts)).toBe(1);
        for (const i of [0, 11])
          expect(
            await (
              await context.request.get(
                `/api/file/uploads?path=existing-${i}.txt`,
              )
            ).text(),
          ).toBe(decision === "approve" ? "new" : "old");
        expect(
          (
            await context.request.get("/api/file/uploads?path=unique.txt")
          ).status(),
        ).toBe(decision === "approve" ? 200 : 404);
      } finally {
        await proxy?.close();
      }
    });
  }
for (const code of ["storage_full", "storage_unavailable"] as const) {
  test(`one useful error message for ${code}`, async ({ page }) => {
    let posts = 0;
    await page.route("**/mfup/sessions/*/batches/*", async (route) => {
      posts++;
      await route.fulfill({
        status: code === "storage_full" ? 507 : 503,
        contentType: "application/json",
        body: JSON.stringify({
          error: code,
          message: code,
          retryable: false,
          phase: "upload",
        }),
      });
    });
    const zone = page.getByTestId("zone-uploads");
    await page
      .getByLabel("Files uploads", { exact: true })
      .setInputFiles(files("data"));
    await expect(zone.getByTestId("status")).toHaveText("Upload stopped");
    await expect(zone.getByRole("alert")).toHaveCount(1);
    await expect(zone.getByRole("alert")).toContainText(
      code === "storage_full" ? "ran out of space" : "could not read or write",
    );
    await expect(zone.getByRole("alert")).not.toContainText("multipart");
    await expect(zone.getByTestId("overwrite-prompt")).toHaveCount(0);
    await page.waitForTimeout(800);
    expect(posts).toBe(1);
  });
}
