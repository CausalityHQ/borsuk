"""Decision and nested rank projection for the sealed width ladder."""

import unittest

from scripts.v186_cosine_width_ladder import decide, project_rank


class CosineWidthLadderTests(unittest.TestCase):
    def test_smallest_passing_width_then_elastic_allowance(self):
        totals = {str(width): {"446": 12740, "672": 12746,
                               "1344": 12750}
                  for width in (32, 64, 128)}
        p05 = {str(width): {"446": 98, "672": 98, "1344": 99}
               for width in (32, 64, 128)}
        totals["64"]["446"] = 12745
        self.assertEqual(decide(totals, p05), {"width": 64, "allowance": 446})
        totals["64"]["446"] = 12744
        self.assertEqual(decide(totals, p05), {"width": 32, "allowance": 672})
        p05["32"]["672"] = 97
        self.assertEqual(decide(totals, p05), {"width": 64, "allowance": 672})
        for arm in totals.values():
            arm["672"] = 12744
        self.assertEqual(decide(totals, p05),
                         {"decision": "revise-candidate-generator"})

    def test_nested_projection_uses_only_selected_units(self):
        all_units = (2, 3, 4, 7)
        minima = (0.4, 0.1, 0.3, 0.2)
        self.assertEqual(project_rank(all_units, minima, (2, 4, 7)),
                         [7, 4, 2])
        with self.assertRaises(ValueError):
            project_rank(all_units, minima, (2, 8))


if __name__ == "__main__":
    unittest.main()
