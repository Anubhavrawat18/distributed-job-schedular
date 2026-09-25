import { Router, type Request, type Response, type NextFunction } from "express";
import { getSnapshot } from "../../observability/queries";

const asyncRoute =
  (handler: (req: Request, res: Response) => Promise<unknown>) =>
  (req: Request, res: Response, next: NextFunction) => {
    handler(req, res).catch(next);
  };

export const statsRouter = Router();

// The dashboard's only data source. Kept as JSON rather than rendering HTML on
// the server so the same endpoint is usable from a terminal or a monitoring
// check, and the page is a client of the API like anything else.
statsRouter.get(
  "/api/stats",

  asyncRoute(async (_req, res) => {
    const snapshot = await getSnapshot();
    // A polling dashboard must never be served a cached snapshot.
    res.set("cache-control", "no-store");
    return res.json(snapshot);
  }),
);
